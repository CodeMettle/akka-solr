package com.github.akkasolr
package client

import java.util.UUID

import com.github.akkasolr.client.ClientConnection.Messages.SolrMessage
import com.github.akkasolr.client.ClientConnection.{InitRequestTimedOut, fsm}
import com.github.akkasolr.client.ClientConnection.fsm.InitialRequest
import org.apache.solr.common.params.{CommonParams, SolrParams}
import spray.can.Http
import spray.can.client.{HostConnectorSettings, ClientConnectionSettings}
import spray.http._

import akka.actor._
import akka.io.IO
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
object ClientConnection {
    def props(uri: Uri) = {
        Props[ClientConnection](new ClientConnection(uri))
    }

    object Messages {
        sealed trait SolrMessage {
            def timeout: FiniteDuration
        }

        case class Select(query: SolrParams, timeout: FiniteDuration = 1.minute) extends SolrMessage
    }

    object fsm {
        sealed trait State
        case object Disconnected extends State
        case object Connecting extends State
        case object TestingConnection extends State
        case object Connected extends State

        case class InitialRequest(req: SolrMessage, replyTo: ActorRef, startTime: Long = System.nanoTime(),
                                  reqId: UUID = UUID.randomUUID)
        case class CCData(hostConn: ActorRef = null, initReq: Option[InitialRequest] = None)
    }

    private case class InitRequestTimedOut(reqId: UUID)
}

class ClientConnection(baseUri: Uri) extends FSM[fsm.State, fsm.CCData] with Stash with ActorLogging {
    startWith(fsm.Disconnected, fsm.CCData())

    private val selectUri = baseUri withPath baseUri.path / "select"
    private val updateUri = baseUri withPath baseUri.path / "update"
    private val pingUri = baseUri withPath baseUri.path ++ Uri.Path(CommonParams.PING_HANDLER)

    private def connSettings = {
        ClientConnectionSettings(context.system).copy(responseChunkAggregationLimit = 0)
    }

    private def hostConnSettings = {
        HostConnectorSettings(context.system).copy(connectionSettings = connSettings)
    }

    private def serviceRequest(connection: ActorRef, request: SolrMessage, requestor: ActorRef) = {
        log.warning("Unimplemented; conn={}, req={}, reply={}", connection, request, requestor)
    }

    private def serviceInitRequest(connection: ActorRef, init: InitialRequest) = {
        val timeToConnect = System.nanoTime() - init.startTime
        log.debug("New timeout is {}", init.req.timeout - timeToConnect.nanos)
        serviceRequest(connection, init.req, init.replyTo)
    }

    whenUnhandled {
        case Event(InitRequestTimedOut(reqId), data) ⇒ data.initReq match {
            case Some(InitialRequest(_, replyTo, _, r)) if r == reqId ⇒
                replyTo ! Status.Failure(new Http.ConnectionException("Didn't connect before request timeout"))
                stay() using data.copy(initReq = None)

            case _ ⇒ stay()
        }

        case Event(Terminated(dead), data) ⇒ if (dead == data.hostConn) {
            log debug "HostConnector actor died"

            data.initReq foreach (_.replyTo !
                Status.Failure(new Http.ConnectionException("Connection closed while trying to establish")))

            goto(fsm.Disconnected) using fsm.CCData()
        } else
            stay()

        case Event(_: SolrMessage, _) ⇒
            stash()
            stay()

        case Event(m, data) ⇒
            val initReqSet = if (data.initReq.isDefined) "exists" else "does not exist"
            val connected = if (data.hostConn != null) "established" else "not established"
            log.warning("Unhandled message, initial request {}; connection {}; message: {}", initReqSet, connected, m)
            stay()
    }

    private def handleConnExc: StateFunction = {
        case Event(f @ Status.Failure(e: Http.ConnectionException), data) ⇒
            log.error(e, "Couldn't connect to {}", baseUri)

            data.initReq foreach (_.replyTo ! f)

            goto(fsm.Disconnected) using fsm.CCData()
    }

    when(fsm.Disconnected) {
        case Event(m: SolrMessage, data) ⇒
            IO(Http)(actorSystem) ! Http
                .HostConnectorSetup(baseUri.authority.host.address, baseUri.effectivePort, baseUri.isSsl,
                settings = Some(hostConnSettings))
            val initReq = InitialRequest(m, sender())
            setTimer(initReq.reqId.toString, InitRequestTimedOut(initReq.reqId), m.timeout)
            goto(fsm.Connecting) using data.copy(initReq = Some(initReq))
    }

    when(fsm.Connecting) (handleConnExc orElse {
        case Event(i: Http.HostConnectorInfo, data) ⇒
            log.debug("Connected; info={}", i)

            context watch sender()

            goto(fsm.TestingConnection) using data.copy(hostConn = sender())
    })

    onTransition {
        case fsm.Connecting -> fsm.Disconnected ⇒
            unstashAll()

        case fsm.Connecting -> fsm.TestingConnection ⇒
            nextStateData.hostConn ! HttpRequest(HttpMethods.GET, pingUri)
    }

    when(fsm.TestingConnection) (handleConnExc orElse {
        case Event(HttpResponse(StatusCodes.OK, _, _, _), data) ⇒
            data.initReq foreach (serviceInitRequest(data.hostConn, _))

            goto(fsm.Connected) using data.copy(initReq = None)

        case Event(HttpResponse(StatusCodes.NotFound, _, _, _), data) ⇒
            data.initReq foreach (_.replyTo ! Status.Failure(
                new Http.ConnectionException(s"$pingUri not found; is '${baseUri.path}' the correct address?")))

            goto(fsm.Disconnected) using fsm.CCData()

        case Event(resp: HttpResponse, data) ⇒
            log.error("{}", resp)
            data.initReq foreach (_.replyTo ! Status.Failure(new Http.ConnectionException("fill this in!@!")))
            goto(fsm.Disconnected) using fsm.CCData()
    })

    onTransition {
        case fsm.TestingConnection -> _ ⇒
            unstashAll()
    }

    when(fsm.Connected) {
        case Event(m: SolrMessage, data) ⇒
            serviceRequest(data.hostConn, m, sender())
            stay()
    }

    initialize()
}
