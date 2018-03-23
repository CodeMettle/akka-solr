/*
 * ClientConnection.scala
 *
 * Updated: Oct 16, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client

import com.codemettle.akkasolr.Solr.SolrOperation
import com.codemettle.akkasolr.client.ClientConnection.{RequestQueue, fsm}
import com.codemettle.akkasolr.solrtypes.SolrQueryResponse
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings, ParserSettings}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, StreamTcpException}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * @author steven
 *
 */
private[akkasolr] object ClientConnection {
    def props(uri: Uri, username: Option[String], password: Option[String])(implicit mat: Materializer) =
        Props(new ClientConnection(uri, username, password))

    object fsm {
        sealed trait State
        case object Disconnected extends State
        case object TestingConnection extends State
        case object Connected extends State

        case class CCData(pingTriesRemaining: Int = 0)
    }

    case class RequestQueue(baseUri: Uri, connPoolSettings: ConnectionPoolSettings)
                           (implicit sys: ActorSystem, mat: Materializer) {
        private val connPool = {
            if (baseUri.isSsl)
                Http().cachedHostConnectionPoolHttps[Promise[HttpResponse]](baseUri.authority.host.address,
                    baseUri.authority.port, settings = connPoolSettings)
            else
                Http().cachedHostConnectionPool[Promise[HttpResponse]](baseUri.authority.host.address,
                    baseUri.authority.port, connPoolSettings)
        }

        private val queue = Source.queue[(HttpRequest, Promise[HttpResponse])](Solr.Client.requestQueueSize, OverflowStrategy.dropNew)
          .via(connPool)
          .toMat(Sink.foreach {
              case ((Success(resp), p)) ⇒ p.success(resp)
              case ((Failure(t), p)) ⇒ p.failure(t)
          })(Keep.left)
          .run()

        def shutdown(): Unit = queue.complete()

        def queueRequest(req: HttpRequest)(implicit ec: ExecutionContext): Future[HttpResponse] = {
            val p = Promise[HttpResponse]()
            queue.offer(req → p) flatMap {
                case QueueOfferResult.Enqueued ⇒ p.future
                case QueueOfferResult.Failure(t) ⇒ Future.failed(t)
                case QueueOfferResult.Dropped ⇒ Future.failed(new Exception(s"Request queue for $baseUri is full"))
                case QueueOfferResult.QueueClosed ⇒ Future.failed(new Exception(s"Request queue for $baseUri is closed"))
            }
        }
    }
}

private[akkasolr] class ClientConnection(baseUri: Uri, username: Option[String], password: Option[String])
                                        (implicit mat: Materializer)
    extends FSM[fsm.State, fsm.CCData] with ActorLogging {

    startWith(fsm.Disconnected, fsm.CCData())

    implicit val system: ActorSystem = context.system

    private val stasher = context.actorOf(ConnectingStasher.props, "stasher")

    private val actorName = Util actorNamer "request"

    private val requestQueue = RequestQueue(baseUri, connPoolSettings)

    override def postStop(): Unit = {
        super.postStop()

        requestQueue.shutdown()
    }

    private def parserSettings = {
        ParserSettings(context.system).withMaxChunkSize(Solr.Client.maxChunkSize)
          .withMaxContentLength(Solr.Client.maxContentLength)
    }

    private def connSettings = {
        ClientConnectionSettings(context.system).withParserSettings(parserSettings)
    }

    private def connPoolSettings =
        ConnectionPoolSettings(context.system).withConnectionSettings(connSettings)

    private def serviceRequest(request: SolrOperation, requestor: ActorRef, timeout: FiniteDuration) = {
        def props = RequestHandler.props(baseUri, username, password, requestQueue, requestor, request, timeout)
        context.actorOf(props, actorName.next())
    }

    private def pingServer() = {
        val req = Solr.Ping()
        serviceRequest(req, self, req.requestTimeout)
    }

    whenUnhandled {
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
        case Event(Status.Failure(e: StreamTcpException), _) ⇒
            log.error(e, "Couldn't connect to {}", baseUri)

            stasher ! ConnectingStasher.ErrorOutAllWaiting(e)

            goto(fsm.Disconnected) using fsm.CCData()
    }

    when(fsm.Disconnected) {
        case Event(m: SolrOperation, _) ⇒
            val to = m.requestTimeout

            stasher ! ConnectingStasher.WaitingRequest(sender(), m, to, to)

            goto(fsm.TestingConnection) using fsm.CCData(pingTriesRemaining = 4)
    }

    onTransition {
        case _ -> fsm.TestingConnection ⇒ pingServer()
    }

    when(fsm.TestingConnection) (handleConnExc orElse {
        case Event(Status.Failure(Solr.RequestTimedOut(_)), data) if data.pingTriesRemaining > 0 ⇒
            log debug "didn't get response from server ping, retrying"
            pingServer()
            stay() using data.copy(pingTriesRemaining = data.pingTriesRemaining - 1)

        case Event(Status.Failure(Solr.RequestTimedOut(_)), _) ⇒
            log warning "Never got a response from server ping, aborting"
            val exc = Solr.ConnectionException("No response to server pings")
            stasher ! ConnectingStasher.ErrorOutAllWaiting(exc)
            goto(fsm.Disconnected) using fsm.CCData()

        case Event(qr: SolrQueryResponse, _) ⇒
            log.debug("got response to ping {}, check status etc?", qr)
            goto(fsm.Connected)

        case Event(Status.Failure(t), _) ⇒
            log.error(t, "Couldn't ping server")
            stasher ! ConnectingStasher.ErrorOutAllWaiting(t)
            goto(fsm.Disconnected) using fsm.CCData()
    })

    onTransition {
        case fsm.TestingConnection -> fsm.Connected ⇒ stasher ! ConnectingStasher.FlushWaitingRequests
    }

    when(fsm.Connected) {
        case Event(m: SolrOperation, _) ⇒
            serviceRequest(m, sender(), m.requestTimeout)
            stay()

        case Event(ConnectingStasher.StashedRequest(act, req: Solr.SolrOperation, remaining, _), _) ⇒
            serviceRequest(req, act, remaining)
            stay()
    }

    initialize()
}
