/*
 * ZkStateReaderWrapper.scala
 *
 * Updated: Oct 13, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.client.zk

import java.io.IOException
import java.util.concurrent.TimeoutException
import java.{lang => jl, util => ju}

import org.apache.solr.client.solrj.SolrRequest
import org.apache.solr.client.solrj.impl.LBHttpSolrServer
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.SolrException
import org.apache.solr.common.cloud._
import org.apache.solr.common.params.{ModifiableSolrParams, SolrParams, UpdateParams}
import org.apache.solr.common.util.{NamedList, StrUtils}
import org.apache.zookeeper.KeeperException

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.client.zk.ZkStateReaderWrapper.RouteResponse
import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.ImmutableSolrParams

import akka.actor.ActorRefFactory
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

/**
 * Class to wrap blocking SolrJ ZkStateReader methods in Futures using the configured ZooKeeper dispatcher.
 *
 * @author steven
 *
 */
object ZkStateReaderWrapper {
    private val nonRoutableParameters = Set(UpdateParams.EXPUNGE_DELETES, UpdateParams.MAX_OPTIMIZE_SEGMENTS,
        UpdateParams.COMMIT, UpdateParams.WAIT_SEARCHER, UpdateParams.OPEN_SEARCHER, UpdateParams.SOFT_COMMIT,
        UpdateParams.PREPARE_COMMIT, UpdateParams.OPTIMIZE)

    case class RouteResponse(routeResponses: Map[String, NamedList[AnyRef]],
                             routes: Map[String, LBHttpSolrServer.Req]) extends NamedList[AnyRef]

    private def condenseResponses(shardResponses: Map[String, NamedList[AnyRef]], timeMillis: Long,
                                  routes: Map[String, LBHttpSolrServer.Req]) = {
        def getStatusFromResp(shardResponse: NamedList[AnyRef]) = {
            def getStatusFromHeader(header: NamedList[_]) = {
                header get "status" match {
                    case shardStatus: jl.Integer ⇒ shardStatus.intValue()
                    case _ ⇒ 0
                }
            }

            shardResponse get "responseHeader" match {
                case header: NamedList[_] ⇒ getStatusFromHeader(header)
                case _ ⇒ 0
            }
        }

        val status = (0 /: shardResponses.values) {
            case (acc, shardResponse) ⇒
                val s = getStatusFromResp(shardResponse)
                if (s > 0) s else acc
        }

        val cheader = new NamedList[AnyRef]()
        cheader.add("status", status: jl.Integer)
        cheader.add("QTime", timeMillis: jl.Long)
        val condensed = RouteResponse(shardResponses, routes)
        condensed.add("responseHeader", cheader)
        condensed
    }
}

class ZkStateReaderWrapper(zkHost: String, config: Solr.SolrCloudConnectionOptions)(implicit arf: ActorRefFactory) {
    private implicit val dispatcher = Solr.Client.zookeeperDispatcher

    def connect: Future[ZkStateReader] = {
        Future {
            val zk = new ZkStateReader(zkHost, config.clientTimeoutMS, config.connectTimeoutMS)
            zk.createClusterStateWatchersAndUpdate()
            zk
        } transform(identity, {
            case zke: ZooKeeperException ⇒ zke
            case e@(_: InterruptedException | _: KeeperException | _: IOException | _: TimeoutException) ⇒
                new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e)
            case t ⇒ t
        })
    }

    def getCollectionSet(reader: ZkStateReader, clusterState: ClusterState, collection: String): Try[Set[String]] = {
        def collectionsForCollectionName(collectionName: String): Try[Set[String]] = {
            if (clusterState.getCollections contains collectionName)
                Success(Set(collectionName))
            else Option(reader.getAliases getCollectionAlias collectionName) match {
                case None ⇒ Failure(Solr.InvalidRequest(s"Collection not found: $collectionName"))
                case Some(alias) ⇒ Try(StrUtils.splitSmart(alias, ",", true).asScala.toSet)
            }
        }

        // Extract each comma separated collection name and store in a List.
        val rawCollectionsList = Try(StrUtils.splitSmart(collection, ",", true).asScala.toSet)

        rawCollectionsList flatMap (collectionNames ⇒ {
            (Try(Set.empty[String]) /: collectionNames) {
                case (f: Failure[_], _) ⇒ f
                case (Success(acc), collectionName) ⇒ collectionsForCollectionName(collectionName) map (cs ⇒ acc ++ cs)
            }
        })
    }

    def buildUrlMap(col: DocCollection): Option[ju.Map[String, ju.List[String]]] = {
        //Create the URL map, which is keyed on slice name.
        //The value is a list of URLs for each replica in the slice.
        //The first value in the list is the leader for the slice.

        def mapEntryFromSlice(collection: DocCollection, slice: Slice): (String, ju.List[String]) = {
            val leader = slice.getLeader
            val zkProps = new ZkCoreNodeProps(leader)
            val urls = Vector(s"${zkProps.getBaseUrl}/${collection.getName}")

            val allUrls = (urls /: slice.getReplicas.asScala) {
                case (acc, replica) if replica.getNodeName != leader.getNodeName && replica.getName != leader.getName ⇒
                    val repProps = new ZkCoreNodeProps(replica)
                    acc :+ s"${repProps.getBaseUrl}/${collection.getName}"
                case (acc, _) ⇒ acc
            }

            slice.getName → allUrls.asJava
        }

        // slices are immutable, so we can do a search for null leader and fail-fast before building the url map

        val slices = col.getActiveSlices.asScala
        slices find (_.getLeader == null) match {
            case Some(_) ⇒
                // take unoptimized general path - we cannot find a leader yet
                None

            case None ⇒
                val urlMap = (slices map (s ⇒ mapEntryFromSlice(col, s))).toMap
                Some(urlMap.asJava)
        }
    }

    def createParams(op: Solr.SolrUpdateOperation): (UpdateRequest, ImmutableSolrParams, ImmutableSolrParams) = {
        val updateRequest = op.solrJUpdateRequest
        val allParams = Option(updateRequest.getParams).fold(ImmutableSolrParams())(ImmutableSolrParams(_))
        val routableParams = ImmutableSolrParams(allParams.params -- ZkStateReaderWrapper.nonRoutableParameters)
        (updateRequest, allParams, routableParams)
    }

    def getDocCollectionForRequest(zkStateReader: ZkStateReader, clusterState: ClusterState,
                                   allParams: ImmutableSolrParams): Try[DocCollection] = {
        def collectionName: Try[String] = {
            Option(allParams.get("collection", config.defaultCollection.orNull)) match {
                case None ⇒
                    Failure(Solr.InvalidRequest(
                        "No collection param specified on request and no default collection has been set"))

                case Some(collName) ⇒ Success {
                    // Check to see if the collection is an alias.

                    (for {
                        aliases ← Option(zkStateReader.getAliases)
                        alias ← aliases.getCollectionAliasMap.asScala get collName
                    } yield alias) getOrElse collName
                }
            }
        }

        collectionName flatMap (collection ⇒ Try(clusterState getCollection collection) recoverWith {
            case t ⇒ Failure(Solr.InvalidRequest(t.getMessage))
        })
    }

    def getRouter(col: DocCollection): Try[Option[DocRouter]] = {

        def router: Try[DocRouter] = Try {
            Option(col.getRouter) getOrElse (throw Solr.InvalidRequest(s"No DocRouter found for ${col.getName}"))
        }

        router map {
            case _: ImplicitDocRouter ⇒ None
            case r ⇒ Some(r)
        }
    }

    def getRoutes(req: UpdateRequest, router: DocRouter, col: DocCollection, urlMap: ju.Map[String, ju.List[String]],
                  routableParams: SolrParams): Try[Option[Map[String, LBHttpSolrServer.Req]]] = {
        Try(req.getRoutes(router, col, urlMap, new ModifiableSolrParams(routableParams), config.idField)) transform
            (r ⇒ Success(Option(r) map (_.asScala.toMap)), t ⇒ Failure(Solr.InvalidRequest(t.getMessage)))
    }

    def getRoutesForRequest(zkStateReader: ZkStateReader, clusterState: ClusterState, req: UpdateRequest,
                            allParams: ImmutableSolrParams,
                            routableParams: ImmutableSolrParams): Try[Option[Map[String, LBHttpSolrServer.Req]]] = {
        def createRoutingInfo(docCol: DocCollection, routerOpt: Option[DocRouter]) = {
            for {
                router ← routerOpt
                urlMap ← buildUrlMap(docCol)
            } yield (docCol, router, urlMap)
        }

        val routingInfo = for {
            docCol ← getDocCollectionForRequest(zkStateReader, clusterState, allParams)
            routerOpt ← getRouter(docCol)
        } yield {
            createRoutingInfo(docCol, routerOpt)
        }

        routingInfo flatMap {
            case None ⇒ Success(None)
            case Some((docCol, router, urlMap)) ⇒ getRoutes(req, router, docCol, urlMap, routableParams)
        }
    }

    def fakeRunRequest(req: LBHttpSolrServer.Req): Future[NamedList[AnyRef]] = {
        ???
    }

    def fakeRunRequests(reqs: Map[String, LBHttpSolrServer.Req]): Future[Map[String, NamedList[AnyRef]]] = {
        // create actor that sends all the requests and collates them into a map or fails with the collated failures
        val errorResponses: Map[String, Throwable] = ???
        val responses: Map[String, NamedList[AnyRef]] = ???

        if (errorResponses.isEmpty)
            Future successful responses
        else
            Future failed Solr.CloudException(errorResponses, reqs)
    }

    def runRoutableUpdates(routes: Map[String, LBHttpSolrServer.Req]): Future[Map[String, NamedList[AnyRef]]] = {
        if (config.parallelUpdates) {
            fakeRunRequests(routes)
        } else {
            // probably replace this with an actor that runs requests sequentially and fails on first error

            // futures, so not really head-recursive
            def loop(acc: Map[String, NamedList[AnyRef]],
                     entries: List[(String, LBHttpSolrServer.Req)]): Future[Map[String, NamedList[AnyRef]]] = {
                if (entries.isEmpty) Future successful acc
                else {
                    val (url, req) = entries.head

                    // only runs next on success, otherwise this failed future is returned
                    fakeRunRequest(req) flatMap (success ⇒ {
                        loop(acc + (url → success), entries.tail)
                    })
                }
            }

            // runs requests sequentially, stops if a failure happens

            loop(Map.empty, routes.toList)
        }
    }

    def runFakeNonRoutableRequest(req: LBHttpSolrServer.Req): Future[LBHttpSolrServer.Rsp] = {
        ???
    }

    def directUpdate(zkStateReader: ZkStateReader, op: Solr.SolrUpdateOperation,
                     clusterState: ClusterState): Future[Option[RouteResponse]] = {
        val (updateRequest, allParams, routableParams) = createParams(op)

        getRoutesForRequest(zkStateReader, clusterState, updateRequest, allParams, routableParams) match {
            case Failure(t) ⇒ Future failed t

            case Success(None) ⇒ Future successful None

            case Success(Some(routes)) if routes.isEmpty ⇒
                Future failed Solr.InvalidRequest("No routes found for request")

            case Success(Some(routes)) ⇒
                val start = System.nanoTime()

                runRoutableUpdates(routes) flatMap (shardResponses ⇒ {
                    val nonRoutableRequest: Option[UpdateRequest] = {
                        Option(updateRequest.getDeleteQuery) match {
                            case None ⇒ None
                            case Some(qs) if qs.isEmpty ⇒ None
                            case Some(qs) ⇒ 
                                val deleteQueryRequest = new UpdateRequest()
                                deleteQueryRequest setDeleteQuery qs
                                Some(deleteQueryRequest)
                        }
                    }

                    val nonRoutableParams = ZkStateReaderWrapper.nonRoutableParameters & allParams.params.keySet

                    def getFinalResponse: Future[Map[String, NamedList[AnyRef]]] = {
                        if (nonRoutableRequest.nonEmpty || nonRoutableParams.nonEmpty) {
                            val request = nonRoutableRequest getOrElse new UpdateRequest
                            request.setParams(new ModifiableSolrParams(allParams))

                            val req = new LBHttpSolrServer.Req(request, Random.shuffle(routes.keySet.toList).asJava)

                            runFakeNonRoutableRequest(req) map (rsp ⇒ {
                                shardResponses + (routes.keys.head → rsp.getResponse)
                            })
                        } else
                            Future successful shardResponses
                    }

                    getFinalResponse map (responses ⇒ {
                        val end = System.nanoTime()

                        Some(ZkStateReaderWrapper.condenseResponses(responses, (end - start).nanos.toMillis, routes))
                    })
                })
        }
    }

    def getSlices(requestCollection: Option[String], zkStateReader: ZkStateReader,
                  clusterState: ClusterState): Try[Map[String, Slice]] = {
        def collection: Try[String] = {
            requestCollection orElse config.defaultCollection match {
                case None ⇒
                    Failure(Solr.InvalidRequest(
                        "No collection param specified on request and no default collection has been set"))

                case Some(c) ⇒ Success(c)
            }
        }

        def collectionList: Try[Set[String]] = {
            def requireNonEmpty(colName: String, colls: Set[String]): Try[Set[String]] = {
                if (colls.nonEmpty) Success(colls)
                else Failure(Solr.InvalidRequest(s"Could not find collection: $colName"))
            }

            for {
                colName ← collection
                collSet ← getCollectionSet(zkStateReader, clusterState, colName)
                collections ← requireNonEmpty(colName, collSet)
            } yield collections
        }

        def mapColSlices(colName: String, colSlices: Iterable[Slice]): Map[String, Slice] = {
            (colSlices map (slice ⇒ s"${colName}_${slice.getName}" → slice)).toMap
        }

        collectionList flatMap (collections ⇒ {
            (Try(Map.empty[String, Slice]) /: collections) {
                case (f: Failure[_], _) ⇒ f
                case (Success(acc), collectionName) ⇒
                    Option(clusterState getActiveSlices collectionName) map (_.asScala) match {
                        case None ⇒ Failure(Solr.InvalidRequest(s"Could not find collection: $collectionName"))
                        case Some(colSlices) ⇒ Success(acc ++ mapColSlices(collectionName, colSlices))
                    }
            }
        })
    }

    def runIndirectRequest(req: Solr.SolrOperation, requestCollection: Option[String], zkStateReader: ZkStateReader,
                           clusterState: ClusterState, sendToLeaders: Boolean): Future[NamedList[AnyRef]] = {

        def createUrlList(slices: Map[String, Slice], liveNodes: Set[String]): Vector[String] = {

            def extractUrlsFromSlice(urls: Vector[String], replicas: Vector[String], nodes: Map[String, Replica],
                                     slice: Slice): (Vector[String], Vector[String], Map[String, Replica]) = {
                ((urls, replicas, nodes) /: slice.getReplicasMap.values().asScala) {
                    case ((urlAcc, replicaAcc, nodesAcc), replica) ⇒
                        val coreNodeProps = new ZkCoreNodeProps(replica)
                        val node = coreNodeProps.getNodeName
                        if (!(liveNodes contains node) || (coreNodeProps.getState != ZkStateReader.ACTIVE))
                            (urlAcc, replicaAcc, nodesAcc)
                        else if (nodesAcc contains node)
                            (urlAcc, replicaAcc, nodesAcc + (node → replica))
                        else {
                            val newNodes = nodesAcc + (node → replica)

                            if (!sendToLeaders || coreNodeProps.isLeader)
                                (urlAcc :+ coreNodeProps.getCoreUrl, replicaAcc, newNodes)
                            else
                                (urlAcc, replicaAcc :+ coreNodeProps.getCoreUrl, newNodes)
                        }
                }
            }

            val (urls, replicas, _) = ((Vector.empty[String], Vector.empty[String], Map.empty[String, Replica]) /:
                slices.values) {
                case ((urlAcc, replicaAcc, nodesAcc), slice) ⇒ extractUrlsFromSlice(urlAcc, replicaAcc, nodesAcc, slice)
            }

            if (!sendToLeaders) {
                Random.shuffle(urls)
            } else {
                Random.shuffle(urls) ++ Random.shuffle(replicas)
            }
        }

        getSlices(requestCollection, zkStateReader, clusterState) match {
            case Failure(t) ⇒ Future failed t
            case Success(slices) ⇒
                val urlList = createUrlList(slices, clusterState.getLiveNodes.asScala.toSet)

                runFakeNonRoutableRequest(new LBHttpSolrServer.Req(req.asInstanceOf[SolrRequest], urlList.asJava)) map (_.getResponse)
        }
    }

    def request(zkStateReader: ZkStateReader, req: Solr.SolrOperation,
                requestCollection: Option[String]): Future[NamedList[AnyRef]] = {
        val clusterState = zkStateReader.getClusterState

        req match {
            case update: Solr.SolrUpdateOperation ⇒ directUpdate(zkStateReader, update, clusterState) flatMap {
                case None ⇒
                    runIndirectRequest(req, requestCollection, zkStateReader, clusterState, sendToLeaders = true)

                case Some(resp) ⇒ Future successful resp
            }

            case _ ⇒ runIndirectRequest(req, requestCollection, zkStateReader, clusterState, sendToLeaders = false)
        }
    }
}
