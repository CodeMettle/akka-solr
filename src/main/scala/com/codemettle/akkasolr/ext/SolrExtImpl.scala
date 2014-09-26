/*
 * SolrExtImpl.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.Solr.SolrConnection
import com.codemettle.akkasolr.ext.SolrExtImpl.scheme
import com.codemettle.akkasolr.manager.Manager
import com.codemettle.akkasolr.util.Util

import akka.ConfigurationException
import akka.actor.{ActorRef, ExtendedActorSystem, Extension}
import akka.pattern.AskTimeoutException
import akka.util.Timeout
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * @author steven
 *
 */
object SolrExtImpl {
    private val scheme = """^https?$""".r
}

class SolrExtImpl(eas: ExtendedActorSystem) extends Extension {
    val config = eas.settings.config getConfig "akkasolr"

    val manager = eas.actorOf(Manager.props, "Solr")

    val responseParserDispatcher = eas.dispatchers lookup "akkasolr.response-parser-dispatcher"

    val maxBooleanClauses = config getInt "solrMaxBooleanClauses"

    val maxChunkSize = {
        val size = config getBytes "sprayMaxChunkSize"
        if (size > Int.MaxValue || size < 0) sys.error("Invalid maxChunkSize")
        size.toInt
    }

    val connectionProvider = {
        val fqcn = config getString "connectionProvider"
        eas.dynamicAccess.createInstanceFor[ConnectionProvider](fqcn, Nil) match {
            case Success(cp) ⇒ cp
            case Failure(e) ⇒
                throw new ConfigurationException(s"Could not find/load Connection Provider class [$fqcn]", e)
        }
    }

    /**
     * Request a Solr connection actor. A connection will be created if needed.
     *
     * === Example ===
     * {{{
     *     override def preStart() = {
     *       super.preStart()
     *
     *       Solr.Client.clientTo("http://my-solr:8983/solr")
     *     }
     *
     *     override def receive = {
     *       case Solr.SolrConnection("http://my-solr:8983/solr", connectionActor) ⇒
     *           // connectionActor available for requests
     *     }
     * }}}
     *
     * @param solrUrl Solr URL to connect to
     * @param requestor Actor to send resulting connection or errors to. Since it is implicit,
     *                  calling this method from inside an actor without specifying `requestor` will use the Actor's
     *                  implicit `self`
     * @throws com.codemettle.akkasolr.Solr.InvalidUrl [[Solr.InvalidUrl]] if `solrUrl` cannot be handled
     * @return Unit; sends a [[Solr.SolrConnection]] message to `requestor`. A `spray.can.Http.ConnectionException`
     *         wrapped in a [[akka.actor.Status.Failure]] may be raised by Spray and sent to `requestor`.
     */
    def clientTo(solrUrl: String)(implicit requestor: ActorRef) = {
        val uri = Util normalize solrUrl
        uri.scheme match {
            case scheme() ⇒
            case _ ⇒ throw Solr.InvalidUrl(solrUrl, s"${uri.scheme} connections not supported")
        }
        manager.tell(Manager.Messages.ClientTo(uri, solrUrl), requestor)
    }

    /**
     * `Ask`s the Solr.Client.manager for a connection actor.
     *
     * @see [[SolrExtImpl.clientTo]]
     * @return a [[Future]] containing the connection's [[com.codemettle.akkasolr.client.ClientConnection]] [[ActorRef]]
     */
    def clientFutureTo(solrUrl: String)(implicit exeCtx: ExecutionContext): Future[ActorRef] = {
        val uri = Util normalize solrUrl
        uri.scheme match {
            case scheme() ⇒
                import akka.pattern.ask
                import scala.concurrent.duration._
                implicit val timeout = Timeout(10.seconds)

                (manager ? Manager.Messages.ClientTo(uri, solrUrl)).mapTo[SolrConnection] transform (_.connection, {
                    case _: AskTimeoutException ⇒ new Exception("Unknown error, no response from Solr Manager")
                    case t ⇒ t
                })

            case _ ⇒ Future failed Solr.InvalidUrl(solrUrl, s"${uri.scheme} connections not supported")
        }
    }
}
