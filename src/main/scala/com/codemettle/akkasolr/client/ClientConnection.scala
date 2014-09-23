/*
 * ClientConnection.scala
 *
 * Updated: Sep 22, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client

import org.apache.solr.client.solrj.response.QueryResponse
import spray.can.Http
import spray.can.client.{ClientConnectionSettings, HostConnectorSettings}
import spray.http._

import com.codemettle.akkasolr.Solr.SolrOperation
import com.codemettle.akkasolr.client.ClientConnection.fsm
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.io.IO
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
private[akkasolr] object ClientConnection {
    def props(uri: Uri) = {
        Props[ClientConnection](new ClientConnection(uri))
    }

    object fsm {
        sealed trait State
        case object Disconnected extends State
        case object Connecting extends State
        case object TestingConnection extends State
        case object Connected extends State

        case class CCData(hostConn: ActorRef = null, pingTriesRemaining: Int = 0)
    }
}

private[akkasolr] class ClientConnection(baseUri: Uri) extends FSM[fsm.State, fsm.CCData] with ActorLogging {
    startWith(fsm.Disconnected, fsm.CCData())

    private val stasher = context.actorOf(ConnectingStasher.props, "stasher")

    private val actorName = Util actorNamer "request"

    private def connSettings = {
        ClientConnectionSettings(context.system).copy(responseChunkAggregationLimit = 0)
    }

    private def hostConnSettings = {
        HostConnectorSettings(context.system).copy(connectionSettings = connSettings)
    }

    private def serviceRequest(hostConn: ActorRef, request: SolrOperation, requestor: ActorRef,
                               timeout: FiniteDuration) = {
        //log.warning("Unimplemented; conn={}, req={}, reply={}", stateData.hostConn, request, requestor)
        context.actorOf(RequestHandler.props(baseUri, hostConn, requestor, request, timeout), actorName.next())
    }

    private def pingServer(hostConn: ActorRef) = {
        val req = Solr.Ping()
        serviceRequest(hostConn, req, self, req.options.requestTimeout)
    }

    whenUnhandled {
        case Event(Terminated(dead), data) ⇒ if (dead == data.hostConn) {
            log debug "HostConnector actor died"

            val exc = new Http.ConnectionException("Connection closed while trying to establish")
            stasher ! ConnectingStasher.ErrorOutAllWaiting(exc)

            goto(fsm.Disconnected) using fsm.CCData()
        } else
            stay()

        case Event(req: SolrOperation, _) ⇒
            stasher ! ConnectingStasher.WaitingRequest(sender(), req, req.options.requestTimeout)
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
        case Event(m: SolrOperation, data) ⇒
            IO(Http)(actorSystem) ! Http.HostConnectorSetup(baseUri.authority.host.address, baseUri.effectivePort,
                baseUri.isSsl, settings = Some(hostConnSettings))

            stasher ! ConnectingStasher.WaitingRequest(sender(), m, m.options.requestTimeout)

            goto(fsm.Connecting) using fsm.CCData()
    }

    when(fsm.Connecting) (handleConnExc orElse {
        case Event(i: Http.HostConnectorInfo, data) ⇒
            log.debug("Connected; info={}", i)

            context watch sender()

            goto(fsm.TestingConnection) using fsm.CCData(sender(), pingTriesRemaining = 4)
    })

    onTransition {
        case fsm.Connecting -> fsm.TestingConnection ⇒ pingServer(nextStateData.hostConn)
    }

    when(fsm.TestingConnection) (handleConnExc orElse {
        case Event(Status.Failure(Solr.RequestTimedOut(_)), data) if data.pingTriesRemaining > 0 ⇒
            log debug "didn't get response from server ping, retrying"
            pingServer(data.hostConn)
            stay() using data.copy(pingTriesRemaining = data.pingTriesRemaining - 1)

        case Event(Status.Failure(Solr.RequestTimedOut(_)), _) ⇒
            log warning "Never got a response from server ping, aborting"
            val exc = new Http.ConnectionException("No response to server pings")
            stasher ! ConnectingStasher.ErrorOutAllWaiting(exc)
            goto(fsm.Disconnected) using fsm.CCData()

        case Event(qr: QueryResponse, _) ⇒
            log.debug("replace this later; connected; {}", qr)
            goto(fsm.Connected)

        case Event(HttpResponse(StatusCodes.OK, _, _, _), data) ⇒ goto(fsm.Connected)

        case Event(HttpResponse(StatusCodes.NotFound, _, _, _), data) ⇒
            /*
            val exc = new Http.ConnectionException(s"$pingUri not found; is '${baseUri.path}' the correct address?")

            stasher ! ConnectingStasher.ErrorOutAllWaiting(exc)
            */

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
        case Event(m: SolrOperation, data) ⇒
            serviceRequest(data.hostConn, m, sender(), m.options.requestTimeout)
            stay()

        case Event(ConnectingStasher.StashedRequest(act, req, remaining), data) ⇒
            serviceRequest(data.hostConn, req, act, remaining)
            stay()
    }

    initialize()
}
