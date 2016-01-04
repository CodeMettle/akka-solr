/*
 * ClientConnection.scala
 *
 * Updated: Oct 16, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client

import spray.can.Http
import spray.can.client.{ClientConnectionSettings, HostConnectorSettings}
import spray.can.parsing.ParserSettings
import spray.http._

import com.codemettle.akkasolr.Solr.SolrOperation
import com.codemettle.akkasolr.client.ClientConnection.fsm
import com.codemettle.akkasolr.solrtypes.SolrQueryResponse
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.io.IO
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
private[akkasolr] object ClientConnection {
    def props(uri: Uri, username: Option[String], password: Option[String]) = {
        Props[ClientConnection](new ClientConnection(uri, username, password))
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

private[akkasolr] class ClientConnection(baseUri: Uri, username: Option[String], password: Option[String])
    extends FSM[fsm.State, fsm.CCData] with ActorLogging {

    startWith(fsm.Disconnected, fsm.CCData())

    private val stasher = context.actorOf(ConnectingStasher.props, "stasher")

    private val actorName = Util actorNamer "request"

    private def parserSettings = {
        ParserSettings(context.system).copy(maxChunkSize = Solr.Client.maxChunkSize)
    }

    private def connSettings = {
        ClientConnectionSettings(context.system)
            .copy(responseChunkAggregationLimit = 0, parserSettings = parserSettings)
    }

    private def hostConnSettings = {
        HostConnectorSettings(context.system).copy(connectionSettings = connSettings)
    }

    private def serviceRequest(hostConn: ActorRef, request: SolrOperation, requestor: ActorRef,
                               timeout: FiniteDuration) = {
        def props = RequestHandler.props(baseUri, username, password, hostConn, requestor, request, timeout)
        context.actorOf(props, actorName.next())
    }

    private def pingServer(hostConn: ActorRef) = {
        val req = Solr.Ping()
        serviceRequest(hostConn, req, self, req.requestTimeout)
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
            val to = req.requestTimeout
            stasher ! ConnectingStasher.WaitingRequest(sender(), req, to, to)
            stay()

        case Event(ConnectingStasher.StashedRequest(act, req, remainingTimeout, origTimeout), _) ⇒
            // if we get this message, it means that we successfully connected, asked for stashed connections, and
            // then got disconnected before processing them all
            stasher ! ConnectingStasher.WaitingRequest(act, req, remainingTimeout, origTimeout)
            stay()

        case Event(m, _) ⇒
            stay() replying Status.Failure(Solr.InvalidRequest(m.toString))
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

            val to = m.requestTimeout

            stasher ! ConnectingStasher.WaitingRequest(sender(), m, to, to)

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

        case Event(qr: SolrQueryResponse, _) ⇒
            log.debug("got response to ping {}, check status etc?", qr)
            goto(fsm.Connected)

        case Event(Status.Failure(t), data) ⇒
            log.error(t, "Couldn't ping server")
            stasher ! ConnectingStasher.ErrorOutAllWaiting(t)
            goto(fsm.Disconnected) using fsm.CCData()
    })

    onTransition {
        case fsm.TestingConnection -> fsm.Connected ⇒ stasher ! ConnectingStasher.FlushWaitingRequests
    }

    when(fsm.Connected) {
        case Event(m: SolrOperation, data) ⇒
            serviceRequest(data.hostConn, m, sender(), m.requestTimeout)
            stay()

        case Event(ConnectingStasher.StashedRequest(act, req: Solr.SolrOperation, remaining, _), data) ⇒
            serviceRequest(data.hostConn, req, act, remaining)
            stay()
    }

    initialize()
}
