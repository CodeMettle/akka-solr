/*
 * ZkRequestHandler.scala
 *
 * Updated: Oct 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client.zk

import org.apache.solr.common.cloud.ZkStateReader

import com.codemettle.akkasolr.Solr.SolrOperation
import com.codemettle.akkasolr.client.zk.ZkRequestHandler.{ParallelDirectUpdateHandler, SerialDirectUpdateHandler, TimedOut, UpdatesResponse}
import com.codemettle.akkasolr.client.zk.ZkUpdateUtil.DirectUpdateInfo
import com.codemettle.akkasolr.client.{LBClientConnection, SolrCloudConnection}
import com.codemettle.akkasolr.solrtypes.SolrQueryResponse

import akka.actor._
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * @author steven
 *
 */
object ZkRequestHandler {
    def props(lbConnection: ActorRef, zkStateReader: ZkStateReader, zkUtil: ZkUtil, zkUpdateUtil: ZkUpdateUtil,
              specificCollection: Option[String], replyTo: ActorRef, reqTimeout: FiniteDuration,
              origTimeout: FiniteDuration) = {
        Props(new ZkRequestHandler(lbConnection, zkStateReader, zkUtil, zkUpdateUtil, specificCollection, replyTo,
            reqTimeout, origTimeout))
    }

    private case object TimedOut

    private case class LBReqFailure(server: String, failure: Throwable)
    private case class LBReqSuccess(server: String, resp: LBClientConnection.ExtendedResponse)

    private class LBReqRunner(lbConnection: ActorRef, server: String, req: LBClientConnection.ExtendedRequest)
        extends Actor {
        lbConnection ! req

        def receive = {
            case Status.Failure(t) ⇒
                context.parent ! LBReqFailure(server, t)
                context stop self

            case resp: LBClientConnection.ExtendedResponse ⇒
                context.parent ! LBReqSuccess(server, resp)
                context stop self
        }
    }

    private object LBReqRunner {
        def props(lbConnection: ActorRef, server: String, req: LBClientConnection.ExtendedRequest) = {
            Props(new LBReqRunner(lbConnection, server, req))
        }
    }

    private case class UpdatesResponse(responses: Map[String, LBClientConnection.ExtendedResponse])

    private class ParallelDirectUpdateHandler(lbConnection: ActorRef, info: DirectUpdateInfo) extends Actor {
        private var errors = Map.empty[String, Throwable]
        private var responses = Map.empty[String, LBClientConnection.ExtendedResponse]
        private var remaining = info.routes

        info.routes foreach {
            case (server, req) ⇒ context actorOf LBReqRunner.props(lbConnection, server, req)
        }

        private def removeAndCheckComplete(server: String) = {
            remaining -= server
            if (remaining.isEmpty) {
                if (errors.nonEmpty)
                    context.parent ! Status.Failure(Solr.CloudException(errors, info.routes))
                else
                    context.parent ! UpdatesResponse(responses)

                context stop self
            }
        }

        def receive = {
            case LBReqFailure(server, t) ⇒
                errors += (server → t)
                removeAndCheckComplete(server)

            case LBReqSuccess(server, resp) ⇒
                responses += (server → resp)
                removeAndCheckComplete(server)
        }
    }

    private object ParallelDirectUpdateHandler {
        def props(lbConnection: ActorRef, info: DirectUpdateInfo) = {
            Props(new ParallelDirectUpdateHandler(lbConnection, info))
        }
    }

    private class SerialDirectUpdateHandler(lbConnection: ActorRef, info: DirectUpdateInfo) extends Actor {
        private var remaining = Queue() ++ info.routes
        private var responses = Map.empty[String, LBClientConnection.ExtendedResponse]
        private var currentServer: String = _

        override def preStart() = {
            super.preStart()

            runNext()
        }

        private def runNext() = {
            if (remaining.isEmpty) {
                context.parent ! UpdatesResponse(responses)
                context stop self
            } else {
                val ((server, req), rest) = remaining.dequeue
                currentServer = server
                remaining = rest

                lbConnection ! req
            }
        }

        def receive = {
            case fail: Status.Failure ⇒
                context.parent ! fail
                context stop self

            case resp: LBClientConnection.ExtendedResponse ⇒
                responses += (currentServer → resp)
                runNext()
        }
    }

    private object SerialDirectUpdateHandler {
        def props(lbConnection: ActorRef, info: DirectUpdateInfo) = {
            Props(new SerialDirectUpdateHandler(lbConnection, info))
        }
    }
}

class ZkRequestHandler(lbConnection: ActorRef, zkStateReader: ZkStateReader, zkUtil: ZkUtil, zkUpdateUtil: ZkUpdateUtil,
                       specificCollection: Option[String], replyTo: ActorRef, reqTimeout: FiniteDuration,
                       origTimeout: FiniteDuration) extends Actor with ActorLogging {
    import context.dispatcher

    private val timeout = actorSystem.scheduler.scheduleOnce(reqTimeout, self, TimedOut)

    override def postStop() = {
        super.postStop()

        timeout.cancel()
    }

    private def sendError(error: Throwable) = {
        log.debug("Sending {} error to {} as {}", error.getClass.getSimpleName, replyTo, context.parent)
        replyTo.tell(Status.Failure(error), context.parent)
        self ! PoisonPill
    }

    private def handleCompleteUpdate(request: Solr.SolrOperation, updateInfo: DirectUpdateInfo,
                                     responses: Map[String, LBClientConnection.ExtendedResponse], startTime: Long) = {
        val endTime = System.nanoTime()
        val routeResp = ZkUpdateUtil.condenseResponses(responses mapValues (_.response.original.getResponse),
            (endTime - startTime).nanos.toMillis, updateInfo.routes)

        val resp = SolrQueryResponse(request, routeResp)

        replyTo.tell(resp, context.parent)

        context stop self
    }

    private def handleRegular(op: Solr.SolrOperation, collection: Option[String], isUpdateRequest: Boolean) = {
        zkUtil.getUrlsForNormalRequest(isUpdateRequest, collection, zkStateReader) match {
            case Failure(t) ⇒ sendError(t)
            case Success(urls) if urls.isEmpty ⇒ sendError(Solr.InvalidRequest("No URLs found for request"))
            case Success(urls) ⇒
                log.debug("Sending request to {}", urls)
                lbConnection ! LBClientConnection.ExtendedRequest(op, urls.toList)
                context become waitingForRegularResponse
        }
    }

    private def initiate(op: SolrOperation, collection: Option[String]) = op match {
        case suo: Solr.SolrUpdateOperation ⇒
            val clusterState = zkStateReader.getClusterState

            zkUpdateUtil.directUpdateRoutes(zkStateReader, suo, clusterState, collection, reqTimeout) match {
                case Failure(t) ⇒ sendError(t)
                case Success(None) ⇒ handleRegular(op, collection, isUpdateRequest = true)
                case Success(Some(updateInfo)) ⇒
                    val startTime = System.nanoTime()

                    if (zkUtil.config.parallelUpdates)
                        context actorOf ParallelDirectUpdateHandler.props(lbConnection, updateInfo)
                    else
                        context actorOf SerialDirectUpdateHandler.props(lbConnection, updateInfo)

                    context become waitingForDirectUpdateResponse(op, updateInfo, startTime)
            }

        case _ ⇒ handleRegular(op, collection, isUpdateRequest = false)
    }

    private def handleTimeout: Receive = {
        case TimedOut ⇒ sendError(Solr.RequestTimedOut(origTimeout))
    }

    def receive = handleTimeout orElse {
        case op: Solr.SolrOperation ⇒ initiate(op, None)

        case SolrCloudConnection.OperateOnCollection(op, collection) ⇒ initiate(op, Some(collection))
    }

    def waitingForRegularResponse: Receive = handleTimeout orElse {
        case Status.Failure(t) ⇒ sendError(t)
        case LBClientConnection.ExtendedResponse(resp, _) ⇒
            replyTo.tell(resp, context.parent)
            context stop self
    }

    def waitingForDirectUpdateResponse(req: Solr.SolrOperation, updateInfo: DirectUpdateInfo,
                                       startTime: Long): Receive = {
        case Status.Failure(t) ⇒ sendError(t)
        case UpdatesResponse(shardResponses) ⇒ zkUpdateUtil.getNonRoutableUpdate(updateInfo, reqTimeout) match {
            case None ⇒ handleCompleteUpdate(req, updateInfo, shardResponses, startTime)
            case Some(lbReq) ⇒
                lbConnection ! lbReq
                context become waitingForRegularUpdateResponse(req, updateInfo, shardResponses, startTime)
        }
    }

    def waitingForRegularUpdateResponse(req: Solr.SolrOperation, updateInfo: DirectUpdateInfo,
                                        shardResponses: Map[String, LBClientConnection.ExtendedResponse],
                                        startTime: Long): Receive = {
        case Status.Failure(t) ⇒ sendError(t)
        case r: LBClientConnection.ExtendedResponse ⇒
            handleCompleteUpdate(req, updateInfo, shardResponses + (r.server → r), startTime)
    }
}
