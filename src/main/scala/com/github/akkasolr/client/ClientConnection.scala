package com.github.akkasolr
package client

import com.github.akkasolr.client.ClientConnection.Messages.SolrMessage
import com.github.akkasolr.client.ClientConnection.fsm
import org.apache.solr.common.params.{CommonParams, SolrParams}
import spray.can.Http
import spray.can.client.{ClientConnectionSettings, HostConnectorSettings}
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

        case class CCData(hostConn: ActorRef = null)
    }
}

class ClientConnection(baseUri: Uri) extends FSM[fsm.State, fsm.CCData] with ActorLogging {
    startWith(fsm.Disconnected, fsm.CCData())

    private val selectUri = baseUri withPath baseUri.path / "select"
    private val updateUri = baseUri withPath baseUri.path / "update"
    private val pingUri = baseUri withPath baseUri.path ++ Uri.Path(CommonParams.PING_HANDLER)

    private val stasher = context.actorOf(ConnectingStasher.props, "stasher")

    private def connSettings = {
        ClientConnectionSettings(context.system).copy(responseChunkAggregationLimit = 0)
    }

    private def hostConnSettings = {
        HostConnectorSettings(context.system).copy(connectionSettings = connSettings)
    }

    private def serviceRequest(request: SolrMessage, requestor: ActorRef, timeout: FiniteDuration) = {
        log.warning("Unimplemented; conn={}, req={}, reply={}", stateData.hostConn, request, requestor)
    }

    whenUnhandled {
        case Event(Terminated(dead), data) ⇒ if (dead == data.hostConn) {
            log debug "HostConnector actor died"

            val exc = new Http.ConnectionException("Connection closed while trying to establish")
            stasher ! ConnectingStasher.ErrorOutAllWaiting(exc)

            goto(fsm.Disconnected) using fsm.CCData()
        } else
            stay()

        case Event(req: SolrMessage, _) ⇒
            stasher ! ConnectingStasher.WaitingRequest(sender(), req, req.timeout)
            stay()

        case Event(ConnectingStasher.StashedRequest(act, req, remainingTimeout), _) ⇒
            // if we get this message, it means that we successfully connected, asked for stashed connections, and
            // then got disconnected before processing them all
            stasher ! ConnectingStasher.WaitingRequest(act, req, remainingTimeout)
            stay()

        case Event(m, data) ⇒
            val connected = if (data.hostConn != null) "established" else "not established"
            log.warning("Unhandled message, connection {}; message: {}", connected, m)
            stay()
    }

    private def handleConnExc: StateFunction = {
        case Event(Status.Failure(e: Http.ConnectionException), data) ⇒
            log.error(e, "Couldn't connect to {}", baseUri)

            stasher ! ConnectingStasher.ErrorOutAllWaiting(e)

            goto(fsm.Disconnected) using fsm.CCData()
    }

    when(fsm.Disconnected) {
        case Event(m: SolrMessage, data) ⇒
            IO(Http)(actorSystem) ! Http.HostConnectorSetup(baseUri.authority.host.address, baseUri.effectivePort,
                baseUri.isSsl/*, settings = Some(hostConnSettings)*/)

            stasher ! ConnectingStasher.WaitingRequest(sender(), m, m.timeout)

            goto(fsm.Connecting) using fsm.CCData()
    }

    when(fsm.Connecting) (handleConnExc orElse {
        case Event(i: Http.HostConnectorInfo, data) ⇒
            log.debug("Connected; info={}", i)

            context watch sender()

            goto(fsm.TestingConnection) using data.copy(hostConn = sender())
    })

    onTransition {
        case fsm.Connecting -> fsm.TestingConnection ⇒ nextStateData.hostConn ! HttpRequest(HttpMethods.GET, pingUri)
    }

    when(fsm.TestingConnection) (handleConnExc orElse {
        case Event(HttpResponse(StatusCodes.OK, _, _, _), data) ⇒ goto(fsm.Connected)

        case Event(HttpResponse(StatusCodes.NotFound, _, _, _), data) ⇒
            val exc = new Http.ConnectionException(s"$pingUri not found; is '${baseUri.path}' the correct address?")

            stasher ! ConnectingStasher.ErrorOutAllWaiting(exc)

            goto(fsm.Disconnected) using fsm.CCData()

        case Event(resp: HttpResponse, data) ⇒
            log.error("{}", resp)
            val exc = new Http.ConnectionException("fill this in!@!")
            stasher ! ConnectingStasher.ErrorOutAllWaiting(exc)
            goto(fsm.Disconnected) using fsm.CCData()
    })

    onTransition {
        case fsm.TestingConnection -> fsm.Connected ⇒ stasher ! ConnectingStasher.FlushWaitingRequests
    }

    when(fsm.Connected) {
        case Event(m: SolrMessage, data) ⇒
            serviceRequest(m, sender(), m.timeout)
            stay()

        case Event(ConnectingStasher.StashedRequest(act, req, remaining), data) ⇒
            serviceRequest(req, act, remaining)
            stay()
    }

    initialize()
}
