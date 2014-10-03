/*
 * Solr.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr

import com.typesafe.config.Config
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.params.SolrParams
import spray.http.StatusCode
import spray.util.SettingsCompanion

import com.codemettle.akkasolr.ext.SolrExtImpl
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.QueryPart
import com.codemettle.akkasolr.querybuilder.{SolrQueryBuilder, SolrQueryStringBuilder}
import com.codemettle.akkasolr.util.Util

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

    /* **** utils *****/

    /**
     * Create a [[com.codemettle.akkasolr.querybuilder.SolrQueryBuilder]]
     * @param q Solr query string
     * @return a [[com.codemettle.akkasolr.querybuilder.SolrQueryBuilder]]
     */
    def createQuery(q: String) = SolrQueryBuilder(q)

    def createQuery(qp: QueryPart)(implicit arf: ActorRefFactory) = SolrQueryBuilder(qp.render)

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

    /**
     * Request that can be sent straight to `Solr.Client.manager`
     *
     * @param baseUri Full Uri to the Solr server (ie http://mysolraddress:8983/solr/core1). If no connection to the
     *                server exists, one will be created.
     * @param op operation to run
     */
    @SerialVersionUID(1L)
    case class Request(baseUri: String, op: SolrOperation) {
        @transient
        val uri = Util normalize baseUri
    }

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
        def apply(query: SolrParams)(implicit arf: ActorRefFactory): Select = {
            new Select(query, RequestOptions(actorSystem))
        }
        def apply(qSBuilder: SolrQueryStringBuilder.QueryPart)(implicit arf: ActorRefFactory): Select = {
            apply(qSBuilder.queryOptions())
        }
        def apply(qBuilder: SolrQueryBuilder)(implicit arf: ActorRefFactory): Select = {
            new Select(qBuilder.toParams, RequestOptions(actorSystem))
        }
        def Streaming(query: SolrParams)(implicit arf: ActorRefFactory): Select = {
            new Select(query, RequestOptions(actorSystem).copy(responseType = SolrResponseTypes.Streaming))
        }
        def Streaming(qSBuilder: SolrQueryStringBuilder.QueryPart)(implicit arf: ActorRefFactory): Select = {
            Streaming(qSBuilder.queryOptions())
        }
        def Streaming(qBuilder: SolrQueryBuilder)(implicit arf: ActorRefFactory): Select = {
            new Select(qBuilder.toParams, RequestOptions(actorSystem).copy(responseType = SolrResponseTypes.Streaming))
        }
    }

    @SerialVersionUID(1L)
    case class Ping(action: Option[Ping.Action], options: RequestOptions) extends SolrOperation {
        def withAction(action: Ping.Action) = copy(action = Some(action))
    }

    @SerialVersionUID(1L)
    case class Commit(waitForSearcher: Boolean, softCommit: Boolean, options: RequestOptions) extends SolrOperation

    object Commit {
        def apply(waitForSearcher: Boolean = true, softCommit: Boolean = false)(implicit arf: ActorRefFactory): Commit = {
            apply(waitForSearcher, softCommit, RequestOptions(actorSystem))
        }
    }

    @SerialVersionUID(1L)
    case class Optimize(waitForSearcher: Boolean, maxSegments: Int, options: RequestOptions) extends SolrOperation

    object Optimize {
        def apply(waitForSearcher: Boolean = true, maxSegments: Int = 1)(implicit arf: ActorRefFactory): Optimize = {
            apply(waitForSearcher, maxSegments, RequestOptions(actorSystem))
        }
    }

    @SerialVersionUID(1L)
    case class Rollback(options: RequestOptions) extends SolrOperation

        @SerialVersionUID(1L)
    case class Update(addDocs: Vector[SolrInputDocument] = Vector.empty, deleteIds: Vector[String] = Vector.empty,
                      deleteQueries: Vector[String] = Vector.empty, updateOptions: UpdateOptions,
                      options: RequestOptions) extends SolrOperation {
        def addDoc(doc: Map[String, AnyRef]) = copy(addDocs = addDocs ++ Util.createSolrInputDocs(doc))

        def addDoc(doc: SolrInputDocument) = copy(addDocs = addDocs :+ doc)

        def deleteById(id: String) = copy(deleteIds = deleteIds :+ id)

        def deleteByQuery(q: String) = copy(deleteQueries = deleteQueries :+ q)

        def deleteByQuery(qb: SolrQueryStringBuilder.QueryPart)(implicit arf: ActorRefFactory) = {
            copy(deleteQueries = deleteQueries :+ qb.render)
        }

        def commit(c: Boolean) = copy(updateOptions = updateOptions.copy(commit = c))

        def commitWithin(c: Duration) = c match {
            case fd: FiniteDuration ⇒ copy(updateOptions = updateOptions.copy(commitWithin = Some(fd)))
            case _ if updateOptions.commitWithin.isEmpty ⇒ this
            case _ ⇒ copy(updateOptions = updateOptions.copy(commitWithin = None))
        }

        def overwrite(o: Boolean) = copy(updateOptions = updateOptions.copy(overwrite = o))

        def withOptions(opts: RequestOptions) = copy(options = opts)

        def withUpdateOptions(opts: UpdateOptions) = copy(updateOptions = opts)
    }

    object Update {
        def AddSolrDocs(docs: SolrInputDocument*)(implicit arf: ActorRefFactory) = {
            Update(addDocs = docs.toVector,
                options = RequestOptions(actorSystem),
                updateOptions = UpdateOptions(actorSystem))
        }

        def AddDocs(docs: Map[String, AnyRef]*)(implicit arf: ActorRefFactory) = {
            Update(addDocs = Util.createSolrInputDocs(docs: _*).toVector,
                options = RequestOptions(actorSystem),
                updateOptions = UpdateOptions(actorSystem))
        }

        def DeleteById(ids: String*)(implicit arf: ActorRefFactory) = {
            Update(deleteIds = ids.toVector,
                options = RequestOptions(actorSystem),
                updateOptions = UpdateOptions(actorSystem))
        }

        def DeleteByQuery(queries: SolrQueryStringBuilder.QueryPart*)(implicit arf: ActorRefFactory) = {
            Update(deleteQueries = (queries map (_.render)).toVector,
                options = RequestOptions(actorSystem),
                updateOptions = UpdateOptions(actorSystem))
        }

        def DeleteByQueryString(queries: String*)(implicit arf: ActorRefFactory) = {
            Update(deleteQueries = queries.toVector,
                options = RequestOptions(actorSystem),
                updateOptions = UpdateOptions(actorSystem))
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

        def apply(action: Ping.Action = null)(implicit arf: ActorRefFactory): Ping = {
            apply(Option(action), RequestOptions(actorSystem).copy(method = RequestMethods.GET, requestTimeout = 5.seconds))
        }
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
    case class RequestOptions(method: RequestMethod,
                              responseType: SolrResponseType,
                              requestTimeout: FiniteDuration)

    object RequestOptions extends SettingsCompanion[RequestOptions]("akkasolr.request-defaults") {
        override def fromSubConfig(c: Config): RequestOptions = {
            apply(
                c getString "method" match {
                    case "GET" ⇒ RequestMethods.GET
                    case "POST" ⇒ RequestMethods.POST
                    case m ⇒ throw new IllegalArgumentException(s"Invalid akkasolr.request-defaults.method: $m")
                },
                c getString "writer-type" match {
                    case "XML" ⇒ SolrResponseTypes.XML
                    case "Binary" ⇒ SolrResponseTypes.Binary
                    case "Streaming" ⇒ SolrResponseTypes.Streaming
                    case m ⇒ throw new IllegalArgumentException(s"Invalid akkasolr.request-defaults.writer-type: $m")
                },
                c.getDuration("request-timeout", MILLISECONDS).millis.toCoarsest match {
                    case fd: FiniteDuration ⇒ fd
                    case o ⇒
                        throw new IllegalArgumentException(s"Invalid akkasolr.request-defaults.request-timeout: $o")
                }
            )
        }
    }

    @SerialVersionUID(1L)
    case class UpdateOptions(commit: Boolean,
                             commitWithin: Option[FiniteDuration],
                             overwrite: Boolean)

    object UpdateOptions extends SettingsCompanion[UpdateOptions]("akkasolr.update-defaults") {
        override def fromSubConfig(c: Config): UpdateOptions = {
            import spray.util.pimpConfig

            apply(
                c getBoolean "commit",
                c getDuration "commit-within" match {
                    case fd: FiniteDuration ⇒ Some(fd)
                    case _ ⇒ None
                },
                c getBoolean "overwrite"
            )
        }
    }
}
