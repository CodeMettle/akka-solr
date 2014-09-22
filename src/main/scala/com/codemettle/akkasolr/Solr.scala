/*
 * Solr.scala
 *
 * Updated: Sep 22, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr

import org.apache.solr.common.params.SolrParams

import com.codemettle.akkasolr.client.SolrQueryBuilder
import com.codemettle.akkasolr.ext.SolrExtImpl
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.QueryPart

import akka.actor._
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

/**
 * @author steven
 *
 */
object Solr extends ExtensionId[SolrExtImpl] with ExtensionIdProvider {
    def Client(implicit arf: ActorRefFactory) = actorSystem registerExtension this

    override def createExtension(system: ExtendedActorSystem) = new SolrExtImpl(system)

    override def lookup() = Solr

    /***** utils *****/

    /**
     * Create a [[SolrQueryBuilder]]
     * @param q Solr query string
     * @return a [[SolrQueryBuilder]]
     */
    def createQuery(q: String) = SolrQueryBuilder(q)

    def createQuery(qp: QueryPart)(implicit arf: ActorRefFactory) = SolrQueryBuilder(SolrQueryStringBuilder render qp)

    /**
     * Create an empty Solr Query String builder
     *
     * {{{
     *     val query = Solr createQuery (Solr.queryBuilder
     *       AND (
     *        field("f1") := 3,
     *        field("f2") :!= "x",
     *        OR (
     *          defaultField() isAnyOf (1, 2),
     *          NOT (field("f3") := 4),
     *          field("time") isInRange (3, 5)
     *        )
     *     )
     *    )
     * }}}
     *
     * @return an empty query string builder
     */
    def queryBuilder = SolrQueryStringBuilder.Empty

    /* **** messages *****/

    @SerialVersionUID(1L)
    case class SolrConnection(forAddress: String, connection: ActorRef)

    sealed trait SolrOperation {
        def timeout: FiniteDuration
    }

    @SerialVersionUID(1L)
    case class Select(query: SolrParams, timeout: FiniteDuration = 1.minute) extends SolrOperation

    @SerialVersionUID(1L)
    case class Ping(action: Option[Ping.Action] = None, timeout: FiniteDuration = 5.seconds) extends SolrOperation

    @SerialVersionUID(1L)
    object Ping {
        @SerialVersionUID(1L)
        sealed trait Action
        @SerialVersionUID(1L)
        case object Enable extends Action
        @SerialVersionUID(1L)
        case object Disable extends Action
    }

    @SerialVersionUID(1L)
    case class RequestTimedOut(after: FiniteDuration)
        extends Exception(s"Request timed out after $after") with NoStackTrace

    @SerialVersionUID(1L)
    case class InvalidResponse(msg: String) extends Exception(s"Couldn't handle response: $msg") with NoStackTrace

    @SerialVersionUID(1L)
    case class ParseError(t: Throwable) extends Exception(s"Error parsing response: ${t.getMessage}") with NoStackTrace
}
