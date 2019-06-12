/*
 * Solr.scala
 *
 * Updated: Oct 14, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr

import java.{lang => jl, util => ju}

import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.params.{SolrParams, UpdateParams}

import com.codemettle.akkasolr.client.LBClientConnection
import com.codemettle.akkasolr.ext.SolrExtImpl
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.{QueryPart, RawQuery}
import com.codemettle.akkasolr.querybuilder.{SolrQueryBuilder, SolrQueryStringBuilder}
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.http.SettingsHack.{LBConnectionOptionsHack, RequestOptionsHack, SolrCloudConnectionOptionsHack, UpdateOptionsHack}
import akka.http.scaladsl.model.StatusCode
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

/**
 * @author steven
 *
 */
object Solr extends ExtensionId[SolrExtImpl] with ExtensionIdProvider {
    def Client(implicit arf: ActorRefFactory): SolrExtImpl = actorSystem registerExtension this

    override def createExtension(system: ExtendedActorSystem) = new SolrExtImpl()(system)

    override def lookup() = Solr

    /* **** utils *****/

    /**
     * Create a [[com.codemettle.akkasolr.querybuilder.SolrQueryBuilder]]
     * @param q Solr query string
     * @return a [[com.codemettle.akkasolr.querybuilder.SolrQueryBuilder]]
     */
    def createQuery(q: String) = SolrQueryBuilder(RawQuery(q))

    def createQuery(qp: QueryPart)(implicit arf: ActorRefFactory) = SolrQueryBuilder(qp)

    /* **** messages *****/

    /**
     * Request that can be sent straight to `Solr.Client.manager`
     *
     * @param baseUri Full Uri to the Solr server (ie http://mysolraddress:8983/solr/core1). If no connection to the
     *                server exists, one will be created.
     * @param op operation to run
     */
    @SerialVersionUID(1L)
    case class Request(baseUri: String, op: SolrOperation, username: Option[String] = None, password: Option[String] = None) {
        @transient
        val uri = Util normalize baseUri
    }

    @SerialVersionUID(1L)
    case class SolrConnection(forAddress: String, connection: ActorRef)

    @SerialVersionUID(1L)
    case class SolrLBConnection(forAddresses: Set[String], connection: ActorRef)

    sealed trait SolrOperation {
        def options: RequestOptions

        def requestTimeout: FiniteDuration = options.requestTimeout

        def withOptions(opts: RequestOptions): SolrOperation

        def withTimeout(fd: FiniteDuration): SolrOperation = withOptions(options.copy(requestTimeout = fd))
    }

    sealed trait SolrUpdateOperation extends SolrOperation {
        def solrJUpdateRequest: UpdateRequest
    }

    object SolrUpdateOperation {
        private[akkasolr] def apply(ur: UpdateRequest)(implicit arf: ActorRefFactory): SolrUpdateOperation = {
            if (Option(ur.getParams).fold(false)(_.getBool(UpdateParams.ROLLBACK, false)))
                Rollback(ur)
            else if (ur.getAction == ACTION.OPTIMIZE)
                Optimize(ur)
            else
                Update(ur)
        }
    }

    @SerialVersionUID(1L)
    case class Select(query: SolrParams, options: RequestOptions) extends SolrOperation {
        def withOptions(opts: RequestOptions): Select = copy(options = opts)

        override def withTimeout(d: FiniteDuration): Select = copy(options = options.copy(requestTimeout = d))

        def withRequestMethod(rm: RequestMethod): Select = copy(options = options.copy(method = rm))

        def withResponseType(srt: SolrResponseType): Select = copy(options = options.copy(responseType = srt))

        def streaming: Select = copy(options = options.copy(responseType = SolrResponseTypes.Streaming))
    }

    object Select {
        def apply(query: SolrParams)(implicit arf: ActorRefFactory): Select = {
            new Select(query, RequestOptions.materialize)
        }
        def apply(qSBuilder: SolrQueryStringBuilder.QueryPart)(implicit arf: ActorRefFactory): Select = {
            apply(qSBuilder.queryOptions())
        }
        def apply(qBuilder: SolrQueryBuilder)(implicit arf: ActorRefFactory): Select = {
            new Select(qBuilder.toParams, RequestOptions.materialize)
        }
        def Streaming(query: SolrParams)(implicit arf: ActorRefFactory): Select = {
            new Select(query, RequestOptions.materialize.copy(responseType = SolrResponseTypes.Streaming))
        }
        def Streaming(qSBuilder: SolrQueryStringBuilder.QueryPart)(implicit arf: ActorRefFactory): Select = {
            Streaming(qSBuilder.queryOptions())
        }
        def Streaming(qBuilder: SolrQueryBuilder)(implicit arf: ActorRefFactory): Select = {
            new Select(qBuilder.toParams, RequestOptions.materialize.copy(responseType = SolrResponseTypes.Streaming))
        }
    }

    @SerialVersionUID(1L)
    case class Ping(action: Option[Ping.Action], options: RequestOptions) extends SolrOperation {
        def withAction(action: Ping.Action): Ping = copy(action = Some(action))

        override def withOptions(opts: RequestOptions): Ping = copy(options = opts)
    }

    @SerialVersionUID(1L)
    case class Commit(waitForSearcher: Boolean, softCommit: Boolean, options: RequestOptions) extends SolrUpdateOperation {
        override def withOptions(opts: RequestOptions): Commit = copy(options = opts)

        override def solrJUpdateRequest: UpdateRequest = {
            val ur = new UpdateRequest()
            ur.setAction(ACTION.COMMIT, true, waitForSearcher, softCommit)
            ur
        }
    }

    object Commit {
        def apply(waitForSearcher: Boolean = true, softCommit: Boolean = false)(implicit arf: ActorRefFactory): Commit = {
            apply(waitForSearcher, softCommit, RequestOptions.materialize)
        }
    }

    @SerialVersionUID(1L)
    case class Optimize(waitForSearcher: Boolean, maxSegments: Int, options: RequestOptions) extends SolrUpdateOperation {
        override def withOptions(opts: RequestOptions): Optimize = copy(options = opts)

        override def solrJUpdateRequest: UpdateRequest = {
            val ur = new UpdateRequest()
            ur.setAction(ACTION.OPTIMIZE, true, waitForSearcher, maxSegments)
            ur
        }
    }

    object Optimize {
        def apply(waitForSearcher: Boolean = true, maxSegments: Int = 1)(implicit arf: ActorRefFactory): Optimize = {
            apply(waitForSearcher, maxSegments, RequestOptions.materialize)
        }

        private[akkasolr] def apply(ur: UpdateRequest)(implicit arf: ActorRefFactory): Optimize = {
            if (ur.getAction != ACTION.OPTIMIZE)
                throw Solr.InvalidRequest("Request isn't an optimize request")

            apply(
                ur.getParams.getBool(UpdateParams.WAIT_SEARCHER, true),
                ur.getParams.getInt(UpdateParams.MAX_OPTIMIZE_SEGMENTS, 1),
                RequestOptions.materialize
            )
        }
    }

    @SerialVersionUID(1L)
    case class Rollback(options: RequestOptions) extends SolrUpdateOperation {
        override def withOptions(opts: RequestOptions): Rollback = copy(options = opts)

        override def solrJUpdateRequest: UpdateRequest = {
            val ur = new UpdateRequest()
            ur.rollback()
            ur
        }
    }

    object Rollback {
        def apply()(implicit arf: ActorRefFactory): Rollback = Rollback(RequestOptions.materialize)

        private[akkasolr] def apply(ur: UpdateRequest)(implicit arf: ActorRefFactory): Rollback = {
            if (!Option(ur.getParams).fold(false)(_.getBool(UpdateParams.ROLLBACK, false)))
                throw Solr.InvalidRequest("Request isn't a rollback request")

            apply(RequestOptions.materialize)
        }
    }

    @SerialVersionUID(1L)
    case class Update(addDocs: Vector[SolrInputDocument] = Vector.empty, deleteIds: Vector[String] = Vector.empty,
                      deleteQueries: Vector[String] = Vector.empty, updateOptions: UpdateOptions,
                      options: RequestOptions) extends SolrUpdateOperation {
        def addDoc(doc: Map[String, AnyRef]): Update = copy(addDocs = addDocs ++ Util.createSolrInputDocs(doc))

        def addDoc(doc: SolrInputDocument): Update = copy(addDocs = addDocs :+ doc)

        def deleteById(id: String): Update = copy(deleteIds = deleteIds :+ id)

        def deleteByQuery(q: String): Update = copy(deleteQueries = deleteQueries :+ q)

        def deleteByQuery(qb: SolrQueryStringBuilder.QueryPart)(implicit arf: ActorRefFactory): Update = {
            copy(deleteQueries = deleteQueries :+ qb.render)
        }

        def commit(c: Boolean): Update = copy(updateOptions = updateOptions.copy(commit = c))

        def commitWithin(c: Duration): Update = c match {
            case fd: FiniteDuration => copy(updateOptions = updateOptions.copy(commitWithin = Some(fd)))
            case _ if updateOptions.commitWithin.isEmpty => this
            case _ => copy(updateOptions = updateOptions.copy(commitWithin = None))
        }

        def overwrite(o: Boolean): Update = copy(updateOptions = updateOptions.copy(overwrite = o))

        def withOptions(opts: RequestOptions): Update = copy(options = opts)

        def withUpdateOptions(opts: UpdateOptions): Update = copy(updateOptions = opts)

        def basicUpdateRequest: UpdateRequest = {
            import CollectionConverters._

            val ur = new UpdateRequest
            updateOptions.commitWithin match {
                case None => addDocs foreach (ur.add(_, updateOptions.overwrite))
                case Some(cw) =>
                    val cwMillis = math.min(Int.MaxValue, cw.toMillis).toInt
                    ur.setCommitWithin(cwMillis)
                    addDocs foreach (ur.add(_, cwMillis, updateOptions.overwrite))
            }
            if (deleteIds.nonEmpty)
                ur.deleteById(deleteIds.asJava)
            if (deleteQueries.nonEmpty)
                ur.setDeleteQuery(deleteQueries.asJava)
            ur
        }

        override def solrJUpdateRequest: UpdateRequest = {
            if (updateOptions.commit) {
                val ur = basicUpdateRequest
                ur.setAction(ACTION.COMMIT, true, true)
                ur
            } else
                basicUpdateRequest
        }
    }

    object Update {
        def AddSolrDocs(docs: SolrInputDocument*)(implicit arf: ActorRefFactory): Update = {
            Update(addDocs = docs.toVector,
                options = RequestOptions.materialize,
                updateOptions = UpdateOptions.materialize)
        }

        def AddDocs(docs: Map[String, AnyRef]*)(implicit arf: ActorRefFactory): Update = {
            Update(addDocs = Util.createSolrInputDocs(docs: _*).toVector,
                options = RequestOptions.materialize,
                updateOptions = UpdateOptions.materialize)
        }

        def DeleteById(ids: String*)(implicit arf: ActorRefFactory): Update = {
            Update(deleteIds = ids.toVector,
                options = RequestOptions.materialize,
                updateOptions = UpdateOptions.materialize)
        }

        def DeleteByQuery(queries: SolrQueryStringBuilder.QueryPart*)(implicit arf: ActorRefFactory): Update = {
            Update(deleteQueries = (queries map (_.render)).toVector,
                options = RequestOptions.materialize,
                updateOptions = UpdateOptions.materialize)
        }

        def DeleteByQueryString(queries: String*)(implicit arf: ActorRefFactory): Update = {
            Update(deleteQueries = queries.toVector,
                options = RequestOptions.materialize,
                updateOptions = UpdateOptions.materialize)
        }

        /**
         * Not really meant for full-fledged conversion, just a way to convert an UpdateRequest that was created by akka-solr
         */
        private[akkasolr] def apply(ur: UpdateRequest)(implicit arf: ActorRefFactory): Update = {
            import CollectionConverters._

            if (ur.getAction == ACTION.OPTIMIZE)
                throw Solr.InvalidRequest("Solr.Update doesn't currently support optimizing; use Solr.Optimize")

            if (Option(ur.getParams).fold(false)(_.getBool(UpdateParams.ROLLBACK, false)))
                throw Solr.InvalidRequest("Solr.Update doesn't currently support rollback; use Solr.Rollback")

            def overwriteDisabled = {
                Option(ur.getDocumentsMap).fold(false)(_.values().asScala forall {
                    case null => false
                    case docOpts => docOpts.asScala get UpdateRequest.OVERWRITE match {
                        case None => false
                        case Some(ow: jl.Boolean) => !ow
                        case _ => false
                    }
                })
            }

            def commitWithinFromDocs = {
                def commitWithinFromDocProps(props: ju.Map[String, AnyRef]) = {
                    Option(props) flatMap (_.asScala get UpdateRequest.COMMIT_WITHIN match {
                        case None => None
                        case Some(cw: jl.Integer) => Some(cw.intValue().millis)
                        case _ => None
                    })
                }

                def uniqueCommitWithinsFromDocuments(docs: ju.Map[SolrInputDocument, ju.Map[String, AnyRef]]) = {
                    (docs.values().asScala map commitWithinFromDocProps).toSet
                }

                val commitWithinOpts = Option(ur.getDocumentsMap)
                    .fold(Set.empty[Option[FiniteDuration]])(uniqueCommitWithinsFromDocuments)
                val commitWithins = commitWithinOpts.flatten

                // if both have 1 unique entry which is a Some(<duration>)/<duration>, that means every document
                // had a commitwithin and they were all the same
                if (commitWithinOpts.size == 1 && commitWithins.size == 1) commitWithins.headOption else None
            }

            val commitWithin = {
                if (ur.getCommitWithin > 0)
                    Some(ur.getCommitWithin.millis)
                else
                    commitWithinFromDocs
            }

            val commit = ur.getAction == ACTION.COMMIT

            val docsToAdd = Option(ur.getDocuments).fold(Vector.empty[SolrInputDocument])(_.asScala.toVector)
            val idsToDelete = Option(ur.getDeleteById).fold(Vector.empty[String])(_.asScala.toVector)
            val deleteQueries = Option(ur.getDeleteQuery).fold(Vector.empty[String])(_.asScala.toVector)

            Update(docsToAdd, idsToDelete, deleteQueries,
                UpdateOptions.materialize.copy(commit, commitWithin, !overwriteDisabled),
                RequestOptions.materialize)
        }
    }

    /* **** errors *****/

    sealed trait AkkaSolrError

    @SerialVersionUID(1L)
    case class ConnectionException(msg: String) extends Exception(msg) with NoStackTrace with AkkaSolrError

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

    @SerialVersionUID(1L)
    case class UpdateError(code: Int, errorMessage: Option[String])
      extends Exception(s"${errorMessage.getOrElse("Unknown error")}, code: $code") with NoStackTrace with AkkaSolrError

    @SerialVersionUID(1L)
    case class AllServersDead()
        extends Exception("No live servers are available to service request") with NoStackTrace with AkkaSolrError

    @SerialVersionUID(1L)
    case class CloudException(exceptions: Map[String, Throwable],
                              routes: Map[String, LBClientConnection.ExtendedRequest])
        extends Exception(s"Error(s) while running SolrCloud update - ${exceptions.values.head.getMessage}",
                          exceptions.values.head) with NoStackTrace with AkkaSolrError

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
            apply(Option(action), RequestOptions.materialize.copy(method = RequestMethods.GET, requestTimeout = 5.seconds))
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

    object RequestOptions {
        def materialize(implicit arf: ActorRefFactory): RequestOptions = RequestOptionsHack(actorSystem)
    }

    @SerialVersionUID(1L)
    case class UpdateOptions(commit: Boolean,
                             commitWithin: Option[FiniteDuration],
                             overwrite: Boolean,
                             failOnNonZeroStatus: Boolean)

    object UpdateOptions {
        def materialize(implicit arf: ActorRefFactory): UpdateOptions = UpdateOptionsHack(actorSystem)
    }

    @SerialVersionUID(1L)
    case class LBConnectionOptions(aliveCheckInterval: FiniteDuration, nonStandardPingLimit: Int)

    object LBConnectionOptions {
        def materialize(implicit arf: ActorRefFactory): LBConnectionOptions = LBConnectionOptionsHack(actorSystem)
    }

    @SerialVersionUID(1L)
    case class SolrCloudConnectionOptions(zkConnectTimeout: FiniteDuration,
                                          zkClientTimeout: FiniteDuration,
                                          connectAtStart: Boolean,
                                          defaultCollection: Option[String],
                                          parallelUpdates: Boolean,
                                          idField: String) {
        private def intDuration(fd: FiniteDuration) = fd.toMillis match {
            case ms if ms > Int.MaxValue => sys.error(s"$fd is too large")
            case ms => ms.toInt
        }

        require(zkConnectTimeout.toMillis < Int.MaxValue, "zookeeper-connect-timeout must be < ~24 days")
        require(zkClientTimeout.toMillis < Int.MaxValue, "zookeeper-client-timeout must be < ~24 days")

        def connectTimeoutMS: Int = intDuration(zkConnectTimeout)
        def clientTimeoutMS: Int = intDuration(zkClientTimeout)
    }

    object SolrCloudConnectionOptions {
        def materialize(implicit arf: ActorRefFactory): SolrCloudConnectionOptions =
            SolrCloudConnectionOptionsHack(actorSystem)
    }
}
