/*
 * SolrServerClientConnection.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client

import org.apache.solr.client.solrj.SolrServer
import org.apache.solr.client.solrj.request.{UpdateRequest, SolrPing}
import org.apache.solr.common.util.NamedList

import com.codemettle.akkasolr.client.SolrServerClientConnection.ReqHandler
import com.codemettle.akkasolr.solrtypes.SolrQueryResponse
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.pattern._
import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
 * @author steven
 *
 */
object SolrServerClientConnection {
    def props(ss: SolrServer) = {
        Props[SolrServerClientConnection](new SolrServerClientConnection(ss))
    }

    private class ReqHandler(solrServer: SolrServer, req: Solr.SolrOperation, replyTo: ActorRef) extends Actor {
        import context.dispatcher

        val timeout = actorSystem.scheduler.scheduleOnce(req.options.requestTimeout, self, 'timeout)

        override def preStart() = {
            super.preStart()

            handleRequest(req)
        }

        override def postStop() = {
            super.postStop()

            timeout.cancel()
        }

        private def sendError(err: Throwable) = {
            replyTo ! Status.Failure(err)
            self ! PoisonPill
        }

        private def finish(respF: Future[SolrQueryResponse]) = {
            respF pipeTo replyTo
            respF onComplete (_ ⇒ self ! PoisonPill)
        }

        private def runOp(op: ⇒ NamedList[AnyRef]) = {
            finish(Future(op) map (r ⇒ SolrQueryResponse(req, r)))
        }

        private def handleRequest(op: Solr.SolrOperation) = op match {
            case Solr.Ping(act, _) ⇒
                val ping = new SolrPing
                act foreach {
                    case Solr.Ping.Enable ⇒ ping.setActionEnable()
                    case Solr.Ping.Disable ⇒ ping.setActionDisable()
                }

                runOp(solrServer request ping)

            case Solr.Commit(waitSearch, soft, _) ⇒
                runOp(solrServer.commit(true, waitSearch, soft).getResponse)

            case Solr.Optimize(waitSearch, maxSegs, _) ⇒
                runOp(solrServer.optimize(true, waitSearch, maxSegs).getResponse)

            case Solr.Rollback(_) ⇒
                runOp(solrServer.rollback().getResponse)

            case Solr.Select(query, _) ⇒
                finish(Future(solrServer.query(query)) map (r ⇒ SolrQueryResponse(req, r)))

            case Solr.Update(addDocs, deleteIds, deleteQueries, opts, _) ⇒
                val ur = new UpdateRequest
                opts.commitWithin foreach (cw ⇒ ur setCommitWithin cw.toMillis.toInt)
                addDocs foreach (ur.add(_, opts.overwrite))
                if (deleteIds.nonEmpty)
                    ur.deleteById(deleteIds.asJava)
                if (deleteQueries.nonEmpty)
                    ur.setDeleteQuery(deleteQueries.asJava)

                if (opts.commit) {
                    // run update then commit then return update result
                    val respF = Future(solrServer request ur) flatMap (updateRes ⇒ {
                        Future(solrServer.commit()) map (_ ⇒ SolrQueryResponse(req, updateRes))
                    })
                    finish(respF)
                } else
                    runOp(solrServer request ur)
        }

        def receive = {
            case 'timeout ⇒ sendError(Solr.RequestTimedOut(req.options.requestTimeout))
        }
    }
}

private[akkasolr] class SolrServerClientConnection(solrServer: SolrServer) extends Actor {
    private val reqNamer = Util actorNamer "request"

    override def postStop() = {
        super.postStop()

        solrServer.shutdown()
    }

    private def handleRequest(op: Solr.SolrOperation) = {
        val replyTo = sender()
        context.actorOf(Props[ReqHandler](new ReqHandler(solrServer, op, replyTo)), reqNamer.next())
    }

    def receive = {
        case op: Solr.SolrOperation ⇒ handleRequest(op)

        case m ⇒ sender() ! Status.Failure(Solr.InvalidRequest(m.toString))
    }
}
