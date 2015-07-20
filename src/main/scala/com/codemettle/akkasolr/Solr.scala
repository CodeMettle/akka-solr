/*
 * Solr.scala
 *
 * Updated: Oct 14, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr

import java.{lang => jl, util => ju}

import com.typesafe.config.Config
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.params.{SolrParams, UpdateParams}
import spray.http.StatusCode
import spray.util.SettingsCompanion

import com.codemettle.akkasolr.client.LBClientConnection
import com.codemettle.akkasolr.ext.SolrExtImpl
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.{QueryPart, RawQuery}
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
    case class Request(baseUri: String, op: SolrOperation) {
        @transient
        val uri = Util normalize baseUri
    }

    @SerialVersionUID(1L)
    case class SolrConnection(forAddress: String, connection: ActorRef)

    @SerialVersionUID(1L)
    case class SolrLBConnection(forAddresses: Set[String], connection: ActorRef)

    sealed trait SolrOperation {
        def options: RequestOptions

        def requestTimeout = options.requestTimeout

        def withOptions(opts: RequestOptions): SolrOperation

        def withTimeout(fd: FiniteDuration) = withOptions(options.copy(requestTimeout = fd))
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
        def withOptions(opts: RequestOptions) = copy(options = opts)

        override def withTimeout(d: FiniteDuration) = copy(options = options.copy(requestTimeout = d))

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

        override def withOptions(opts: RequestOptions) = copy(options = opts)
    }

    @SerialVersionUID(1L)
    case class Commit(waitForSearcher: Boolean, softCommit: Boolean, options: RequestOptions) extends SolrUpdateOperation {
        override def withOptions(opts: RequestOptions) = copy(options = opts)

        override def solrJUpdateRequest: UpdateRequest = {
            val ur = new UpdateRequest()
            ur.setAction(ACTION.COMMIT, true, waitForSearcher, softCommit)
            ur
        }
    }

    object Commit {
        def apply(waitForSearcher: Boolean = true, softCommit: Boolean = false)(implicit arf: ActorRefFactory): Commit = {
            apply(waitForSearcher, softCommit, RequestOptions(actorSystem))
        }
    }

    @SerialVersionUID(1L)
    case class Optimize(waitForSearcher: Boolean, maxSegments: Int, options: RequestOptions) extends SolrUpdateOperation {
        override def withOptions(opts: RequestOptions) = copy(options = opts)

        override def solrJUpdateRequest: UpdateRequest = {
            val ur = new UpdateRequest()
            ur.setAction(ACTION.OPTIMIZE, true, waitForSearcher, maxSegments)
            ur
        }
    }

    object Optimize {
        def apply(waitForSearcher: Boolean = true, maxSegments: Int = 1)(implicit arf: ActorRefFactory): Optimize = {
            apply(waitForSearcher, maxSegments, RequestOptions(actorSystem))
        }

        private[akkasolr] def apply(ur: UpdateRequest)(implicit arf: ActorRefFactory): Optimize = {
            if (ur.getAction != ACTION.OPTIMIZE)
                throw Solr.InvalidRequest("Request isn't an optimize request")

            apply(
                ur.getParams.getBool(UpdateParams.WAIT_SEARCHER, true),
                ur.getParams.getInt(UpdateParams.MAX_OPTIMIZE_SEGMENTS, 1),
                RequestOptions(actorSystem)
            )
        }
    }

    @SerialVersionUID(1L)
    case class Rollback(options: RequestOptions) extends SolrUpdateOperation {
        override def withOptions(opts: RequestOptions) = copy(options = opts)

        override def solrJUpdateRequest: UpdateRequest = {
            val ur = new UpdateRequest()
            ur.rollback()
            ur
        }
    }

    object Rollback {
        def apply()(implicit arf: ActorRefFactory): Rollback = Rollback(RequestOptions(actorSystem))

        private[akkasolr] def apply(ur: UpdateRequest)(implicit arf: ActorRefFactory): Rollback = {
            if (!Option(ur.getParams).fold(false)(_.getBool(UpdateParams.ROLLBACK, false)))
                throw Solr.InvalidRequest("Request isn't a rollback request")

            apply(RequestOptions(actorSystem))
        }
    }

    @SerialVersionUID(1L)
    case class Update(addDocs: Vector[SolrInputDocument] = Vector.empty, deleteIds: Vector[String] = Vector.empty,
                      deleteQueries: Vector[String] = Vector.empty, updateOptions: UpdateOptions,
                      options: RequestOptions) extends SolrUpdateOperation {
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

        def basicUpdateRequest: UpdateRequest = {
            import scala.collection.JavaConverters._

            val ur = new UpdateRequest
            updateOptions.commitWithin match {
                case None ⇒ addDocs foreach (ur.add(_, updateOptions.overwrite))
                case Some(cw) ⇒
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

        /**
         * Not really meant for full-fledged conversion, just a way to convert an UpdateRequest that was created by akka-solr
         */
        private[akkasolr] def apply(ur: UpdateRequest)(implicit arf: ActorRefFactory): Update = {
            import scala.collection.JavaConverters._

            if (ur.getAction == ACTION.OPTIMIZE)
                throw Solr.InvalidRequest("Solr.Update doesn't currently support optimizing; use Solr.Optimize")

            if (Option(ur.getParams).fold(false)(_.getBool(UpdateParams.ROLLBACK, false)))
                throw Solr.InvalidRequest("Solr.Update doesn't currently support rollback; use Solr.Rollback")

            def overwriteDisabled = {
                Option(ur.getDocumentsMap).fold(false)(_.values().asScala forall {
                    case null ⇒ false
                    case docOpts ⇒ docOpts.asScala get UpdateRequest.OVERWRITE match {
                        case None ⇒ false
                        case Some(ow: jl.Boolean) ⇒ !ow
                        case _ ⇒ false
                    }
                })
            }

            def commitWithinFromDocs = {
                def commitWithinFromDocProps(props: ju.Map[String, AnyRef]) = {
                    Option(props) flatMap (_.asScala get UpdateRequest.COMMIT_WITHIN match {
                        case None ⇒ None
                        case Some(cw: jl.Integer) ⇒ Some(cw.intValue().millis)
                        case _ ⇒ None
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

            Update(docsToAdd, idsToDelete, deleteQueries, UpdateOptions(commit, commitWithin, !overwriteDisabled),
                RequestOptions(actorSystem))
        }
    }

    /* **** errors *****/

    sealed trait AkkaSolrError

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
        def materialize(implicit arf: ActorRefFactory) = apply(spray.util.actorSystem)

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
        def materialize(implicit arf: ActorRefFactory) = apply(spray.util.actorSystem)

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

    @SerialVersionUID(1L)
    case class LBConnectionOptions(aliveCheckInterval: FiniteDuration, nonStandardPingLimit: Int)

    object LBConnectionOptions extends SettingsCompanion[LBConnectionOptions]("akkasolr.load-balanced-connection-defaults") {
        def materialize(implicit arf: ActorRefFactory) = apply(spray.util.actorSystem)

        override def fromSubConfig(c: Config): LBConnectionOptions = {
            import spray.util.pimpConfig

            apply(
                c getDuration "alive-check-interval" match {
                    case fd: FiniteDuration ⇒ fd
                    case _ ⇒ 1.minute
                },
                c getInt "non-standard-ping-limit"
            )
        }
    }

    @SerialVersionUID(1L)
    case class SolrCloudConnectionOptions(zkConnectTimeout: FiniteDuration,
                                          zkClientTimeout: FiniteDuration,
                                          connectAtStart: Boolean,
                                          defaultCollection: Option[String],
                                          parallelUpdates: Boolean,
                                          idField: String) {
        private def intDuration(fd: FiniteDuration) = fd.toMillis match {
            case ms if ms > Int.MaxValue ⇒ sys.error(s"$fd is too large")
            case ms ⇒ ms.toInt
        }

        require(zkConnectTimeout.toMillis < Int.MaxValue, "zookeeper-connect-timeout must be < ~24 days")
        require(zkClientTimeout.toMillis < Int.MaxValue, "zookeeper-client-timeout must be < ~24 days")

        def connectTimeoutMS = intDuration(zkConnectTimeout)
        def clientTimeoutMS = intDuration(zkClientTimeout)
    }

    object SolrCloudConnectionOptions extends SettingsCompanion[SolrCloudConnectionOptions]("akkasolr.solrcloud-connection-defaults") {
        def materialize(implicit arf: ActorRefFactory) = apply(spray.util.actorSystem)

        override def fromSubConfig(c: Config): SolrCloudConnectionOptions = {
            import spray.util.pimpConfig

            apply(
                c getDuration "zookeeper-connect-timeout" match {
                    case fd: FiniteDuration ⇒ fd
                    case _ ⇒ 10.seconds
                },
                c getDuration "zookeeper-client-timeout" match {
                    case fd: FiniteDuration ⇒ fd
                    case _ ⇒ 10.seconds
                },
                c getBoolean "connect-at-start",
                Option(c getString "default-collection") flatMap (s ⇒ if (s.trim.isEmpty) None else Some(s)),
                c getBoolean "parallel-updates",
                c getString "id-field"
            )
        }
    }
}
