/*
 * SolrExtImpl.scala
 *
 * Updated: Oct 14, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import spray.http.Uri

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.imperative.ImperativeWrapper
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
class SolrExtImpl(eas: ExtendedActorSystem) extends Extension {
    val config = eas.settings.config getConfig "akkasolr"

    val manager = eas.actorOf(Manager.props, "Solr")

    val responseParserDispatcher = eas.dispatchers lookup "akkasolr.response-parser-dispatcher"

    lazy val zookeeperDispatcher = eas.dispatchers lookup "akkasolr.zookeeper-dispatcher"

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

    def connectionActorProps(uri: Uri) = connectionProvider.connectionActorProps(uri, eas)

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
     *       case Solr.SolrConnection("http://my-solr:8983/solr", connectionActor) =>
     *           // connectionActor available for requests
     *     }
     * }}}
     *
     * @param solrUrl Solr URL to connect to
     * @param requestor Actor to send resulting connection or errors to. Since it is implicit,
     *                  calling this method from inside an actor without specifying `requestor` will use the Actor's
     *                  implicit `self`
     * @return Unit; sends a [[Solr.SolrConnection]] message to `requestor`. A `spray.can.Http.ConnectionException`
     *         wrapped in a [[akka.actor.Status.Failure]] may be raised by Spray and sent to `requestor`.
     */
    def clientTo(solrUrl: String)(implicit requestor: ActorRef) = {
        manager.tell(Manager.Messages.ClientTo(Util normalize solrUrl, solrUrl), requestor)
    }

    /**
     * `Ask`s the Solr.Client.manager for a connection actor.
     *
     * @see [[SolrExtImpl.clientTo]]
     * @return a Future containing the [[com.codemettle.akkasolr.client.ClientConnection]]'s [[ActorRef]]
     */
    def clientFutureTo(solrUrl: String)(implicit exeCtx: ExecutionContext): Future[ActorRef] = {
        import akka.pattern.ask
        import scala.concurrent.duration._
        implicit val timeout = Timeout(10.seconds)

        val uri = Util normalize solrUrl

        (manager ? Manager.Messages.ClientTo(uri, solrUrl)).mapTo[Solr.SolrConnection] transform (_.connection, {
            case _: AskTimeoutException ⇒ new Exception("Unknown error, no response from Solr Manager")
            case t ⇒ t
        })
    }

    /**
     * Creates an [[ImperativeWrapper]], useful for transitioning from other Solr libraries
     *
     * @see [[clientFutureTo]]
     * @return a [[Future]] containing an [[com.codemettle.akkasolr.imperative.ImperativeWrapper]] around the
     *         akka-solr client connection
     */
    def imperativeClientTo(solrUrl: String)(implicit exeCtx: ExecutionContext): Future[ImperativeWrapper] = {
        clientFutureTo(solrUrl) map (a ⇒ ImperativeWrapper(a)(eas))
    }

    // combines any urls that normalize to the same uri
    private def urlsToUriMap(solrUrls: Set[String]) = (Map.empty[Uri, String] /: solrUrls) {
        case (acc, solrUrl) ⇒ acc + ((Util normalize solrUrl) → solrUrl)
    }

    /**
     * Request a LoadBalanced connection that behaves like a [[org.apache.solr.client.solrj.impl.LBHttpSolrServer]].
     * Just like regular connections, a cached connection will be returned if one already exists.
     *
     * @see [[SolrExtImpl.clientTo()]]
     * @param solrUrls set of Solr connections to create. If any URLs in the list resolve to the same
     *                 [[Uri]] from [[Util.normalize( )]], only one connection will be created for each unique Uri.
     * @param options options for the LoadBalanced connection. Can be overridden globally; see
     *                "load-balanced-connection-defaults" in reference.conf
     * @param requestor Actor to send resulting connection to. Since it is implicit,
     *                  calling this method from inside an actor without specifying `requestor` will use the Actor's
     *                  implicit `self`
     * @return Unit; sends a [[Solr.SolrLBConnection]] message to `requestor`
     */
    def loadBalancedClientTo(solrUrls: Set[String], options: Solr.LBConnectionOptions = Solr.LBConnectionOptions(eas))
                            (implicit requestor: ActorRef) = {
        manager.tell(Manager.Messages.LBClientTo(urlsToUriMap(solrUrls), solrUrls, options), requestor)
    }

    def loadBalancedClientFutureTo(solrUrls: Set[String],
                                   options: Solr.LBConnectionOptions = Solr.LBConnectionOptions(eas))
                                  (implicit exeCtx: ExecutionContext): Future[ActorRef] = {
        import akka.pattern.ask
        import scala.concurrent.duration._
        implicit val timeout = Timeout(10.seconds)

        val msg = Manager.Messages.LBClientTo(urlsToUriMap(solrUrls), solrUrls, options)

        (manager ? msg).mapTo[Solr.SolrLBConnection] transform(_.connection, {
            case _: AskTimeoutException ⇒ new Exception("Unknown error, no response from Solr Manager")
            case t ⇒ t
        })
    }

    def loadBalancedImperativeClientTo(solrUrls: Set[String])
                                      (implicit exeCtx: ExecutionContext): Future[ImperativeWrapper] = {
        loadBalancedClientFutureTo(solrUrls) map (a ⇒ ImperativeWrapper(a)(eas))
    }
}
