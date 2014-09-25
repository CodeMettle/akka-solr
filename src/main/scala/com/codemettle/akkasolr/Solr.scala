/*
 * Solr.scala
 *
 * Updated: Sep 25, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr

import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.params.SolrParams
import spray.http.StatusCode

import com.codemettle.akkasolr.ext.SolrExtImpl
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.QueryPart
import com.codemettle.akkasolr.querybuilder.{SolrQueryBuilder, SolrQueryStringBuilder}
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.util.ByteString
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

    /* **** utils *****/

    /**
     * Create a [[com.codemettle.akkasolr.querybuilder.SolrQueryBuilder]]
     * @param q Solr query string
     * @return a [[com.codemettle.akkasolr.querybuilder.SolrQueryBuilder]]
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
    def queryStringBuilder = SolrQueryStringBuilder.Empty

    /* **** messages *****/

    @SerialVersionUID(1L)
    case class SolrConnection(forAddress: String, connection: ActorRef)

    sealed trait SolrOperation {
        def options: RequestOptions
    }

    @SerialVersionUID(1L)
    case class Select(query: SolrParams, options: RequestOptions) extends SolrOperation {
        def withOptions(opts: RequestOptions) = copy(options = opts)

        def withTimeout(d: FiniteDuration) = copy(options = options.copy(requestTimeout = d))

        def withRequestMethod(rm: RequestMethod) = copy(options = options.copy(method = rm))

        def withResponseType(srt: SolrResponseType) = copy(options = options.copy(responseType = srt))

        def streaming = copy(options = options.copy(responseType = SolrResponseTypes.Streaming))
    }

    object Select {
        def apply(query: SolrParams): Select = new Select(query, RequestOptions())
        def apply(qBuilder: SolrQueryBuilder): Select = new Select(qBuilder.toParams, RequestOptions())
        def Streaming(query: SolrParams): Select = {
            new Select(query, RequestOptions(responseType = SolrResponseTypes.Streaming))
        }
        def Streaming(qBuilder: SolrQueryBuilder): Select = {
            new Select(qBuilder.toParams, RequestOptions(responseType = SolrResponseTypes.Streaming))
        }
    }

    @SerialVersionUID(1L)
    case class Ping(action: Option[Ping.Action] = None,
                    options: RequestOptions = RequestOptions(method = RequestMethods.GET, requestTimeout = 5.seconds))
        extends SolrOperation

    @SerialVersionUID(1L)
    case class Commit(waitForSearcher: Boolean = true, softCommit: Boolean = false,
                      options: RequestOptions = RequestOptions()) extends SolrOperation

    @SerialVersionUID(1L)
    case class Optimize(waitForSearcher: Boolean = true, maxSegments: Int = 1,
                        options: RequestOptions = RequestOptions()) extends SolrOperation

    @SerialVersionUID(1L)
    case class Rollback(options: RequestOptions = RequestOptions()) extends SolrOperation

    @SerialVersionUID(1L)
    case class Update(addDocs: Vector[SolrInputDocument] = Vector.empty, deleteIds: Vector[String] = Vector.empty,
                      deleteQueries: Vector[String] = Vector.empty, updateOptions: UpdateOptions = UpdateOptions(),
                      options: RequestOptions = RequestOptions()) extends SolrOperation {
        def addDoc(doc: Map[String, AnyRef]) = copy(addDocs = addDocs ++ Util.createSolrInputDocs(doc))

        def addDoc(doc: SolrInputDocument) = copy(addDocs = addDocs :+ doc)

        def deleteById(id: String) = copy(deleteIds = deleteIds :+ id)

        def deleteByQuery(q: String) = copy(deleteQueries = deleteQueries :+ q)

        def deleteByQuery(qb: SolrQueryStringBuilder.QueryPart)(implicit arf: ActorRefFactory) = {
            copy(deleteQueries = deleteQueries :+ (SolrQueryStringBuilder render qb))
        }

        def commit(c: Boolean) = copy(updateOptions = updateOptions.copy(commit = c))

        def commitWithin(c: Duration) = c match {
            case fd: FiniteDuration ⇒ copy(updateOptions = updateOptions.copy(commitWithin = Some(fd)))
            case _ if updateOptions.commitWithin.isEmpty ⇒ this
            case _ ⇒ copy(updateOptions = updateOptions.copy(commitWithin = None))
        }

        def overwrite(o: Boolean) = copy(updateOptions = updateOptions.copy(overwrite = o))
    }

    object Update {
        def AddSolrDocs(docs: SolrInputDocument*) = {
            Update(addDocs = docs.toVector)
        }

        def AddDocs(docs: Map[String, AnyRef]*) = {
            Update(addDocs = Util.createSolrInputDocs(docs: _*).toVector)
        }

        def DeleteById(ids: String*) = {
            Update(deleteIds = ids.toVector)
        }

        def DeleteByQuery(queries: SolrQueryStringBuilder.QueryPart*)(implicit arf: ActorRefFactory) = {
            Update(deleteQueries = (queries map SolrQueryStringBuilder.render).toVector)
        }

        def DeleteByQueryString(queries: String*) = {
            Update(deleteQueries = queries.toVector)
        }
    }

    /* **** errors *****/

    sealed trait AkkaSolrError

    @SerialVersionUID(1L)
    case class InvalidUrl(url: String, error: String) extends Exception(error) with NoStackTrace with AkkaSolrError

    @SerialVersionUID(1L)
    case class RequestTimedOut(after: FiniteDuration)
        extends Exception(s"Request timed out after $after") with NoStackTrace with AkkaSolrError

    @SerialVersionUID(1L)
    case class InvalidRequest(msg: String) extends Exception(msg) with NoStackTrace with AkkaSolrError

    @SerialVersionUID(1L)
    case class InvalidResponse(msg: String)
        extends Exception(s"Couldn't handle response: $msg") with NoStackTrace with AkkaSolrError

    @SerialVersionUID(1L)
    case class ParseError(t: Throwable)
        extends Exception(s"Error parsing response: ${t.getMessage}") with NoStackTrace with AkkaSolrError

    @SerialVersionUID(1L)
    case class ServerError(status: StatusCode, msg: String)
        extends Exception(s"$status - $msg") with NoStackTrace with AkkaSolrError

    /* **** types *****/

    @SerialVersionUID(1L)
    object Ping {
        @SerialVersionUID(1L)
        sealed trait Action
        @SerialVersionUID(1L)
        case object Enable extends Action
        @SerialVersionUID(1L)
        case object Disable extends Action
    }

    sealed trait RequestMethod

    @SerialVersionUID(1L)
    object RequestMethods {
        @SerialVersionUID(1L)
        case object GET extends RequestMethod
        @SerialVersionUID(1L)
        case object POST extends RequestMethod
    }

    sealed trait SolrResponseType

    @SerialVersionUID(1L)
    object SolrResponseTypes {
        @SerialVersionUID(1L)
        case object XML extends SolrResponseType
        @SerialVersionUID(1L)
        case object Binary extends SolrResponseType
        @SerialVersionUID(1L)
        case object Streaming extends SolrResponseType
    }

    @SerialVersionUID(1L)
    case class RequestOptions(method: RequestMethod = RequestMethods.POST,
                              responseType: SolrResponseType = SolrResponseTypes.Binary,
                              requestTimeout: FiniteDuration = 1.minute)

    @SerialVersionUID(1L)
    case class UpdateOptions(commit: Boolean = false, commitWithin: Option[FiniteDuration] = None,
                             overwrite: Boolean = true)

}
