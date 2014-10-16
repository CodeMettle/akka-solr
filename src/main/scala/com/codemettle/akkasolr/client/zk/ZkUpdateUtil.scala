/*
 * ZkUpdateUtil.scala
 *
 * Updated: Oct 16, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.client.zk

import java.{lang => jl, util => ju}

import org.apache.solr.client.solrj.impl.LBHttpSolrServer
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.cloud._
import org.apache.solr.common.params.{ModifiableSolrParams, SolrParams, UpdateParams}
import org.apache.solr.common.util.NamedList

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.client.LBClientConnection
import com.codemettle.akkasolr.client.SolrCloudConnection.RouteResponse
import com.codemettle.akkasolr.client.zk.ZkUpdateUtil.{DirectUpdateInfo, nonRoutableParameters}
import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.ImmutableSolrParams

import akka.actor.ActorRefFactory
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Random, Success, Try}

/**
 * Translates ZooKeeper-specific code from [[org.apache.solr.client.solrj.impl.CloudSolrServer]]
 *
 * @author steven
 *
 */
object ZkUpdateUtil {
    private val nonRoutableParameters = Set(UpdateParams.EXPUNGE_DELETES, UpdateParams.MAX_OPTIMIZE_SEGMENTS,
        UpdateParams.COMMIT, UpdateParams.WAIT_SEARCHER, UpdateParams.OPEN_SEARCHER, UpdateParams.SOFT_COMMIT,
        UpdateParams.PREPARE_COMMIT, UpdateParams.OPTIMIZE)

    private[zk] case class DirectUpdateInfo(updateRequest: UpdateRequest, allParams: ImmutableSolrParams,
                                            routes: Map[String, LBClientConnection.ExtendedRequest])

    def condenseResponses(shardResponses: Map[String, NamedList[AnyRef]], timeMillis: Long,
                          routes: Map[String, LBClientConnection.ExtendedRequest]) = {
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

case class ZkUpdateUtil(config: Solr.SolrCloudConnectionOptions)(implicit arf: ActorRefFactory) {
    private def buildUrlMap(col: DocCollection): Option[ju.Map[String, ju.List[String]]] = {
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

    private def createParams(op: Solr.SolrUpdateOperation): (UpdateRequest, ImmutableSolrParams, ImmutableSolrParams) = {
        val updateRequest = op.solrJUpdateRequest
        val allParams = Option(updateRequest.getParams).fold(ImmutableSolrParams())(ImmutableSolrParams(_))
        val routableParams = ImmutableSolrParams(allParams.params -- nonRoutableParameters)
        (updateRequest, allParams, routableParams)
    }

    private def getRouter(col: DocCollection): Try[Option[DocRouter]] = {
        val router = Option(col.getRouter).fold[Try[DocRouter]](
            Failure(Solr.InvalidRequest(s"No DocRouter found for ${col.getName}")))(Success(_))

        router map {
            case _: ImplicitDocRouter ⇒ None
            case r ⇒ Some(r)
        }
    }

    private def getDocCollectionForRequest(zkStateReader: ZkStateReader, clusterState: ClusterState,
                                           collection: Option[String]): Try[DocCollection] = {
        def collectionName: Try[String] = collection orElse config.defaultCollection match {
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

        collectionName flatMap (collection ⇒ Try(clusterState getCollection collection) recoverWith {
            case t ⇒ Failure(Solr.InvalidRequest(t.getMessage))
        })
    }

    private def getRoutesForRequest(zkStateReader: ZkStateReader, clusterState: ClusterState, req: UpdateRequest,
                                    collection: Option[String],
                                    routableParams: ImmutableSolrParams): Try[Option[Map[String, LBClientConnection.ExtendedRequest]]] = {
        def createRoutingInfo(docCol: DocCollection, routerOpt: Option[DocRouter]) = {
            for {
                router ← routerOpt
                urlMap ← buildUrlMap(docCol)
            } yield (docCol, router, urlMap)
        }

        val routingInfo = for {
            docCol ← getDocCollectionForRequest(zkStateReader, clusterState, collection)
            routerOpt ← getRouter(docCol)
        } yield {
            createRoutingInfo(docCol, routerOpt)
        }

        routingInfo flatMap {
            case None ⇒ Success(None)
            case Some((docCol, router, urlMap)) ⇒ getRoutes(req, router, docCol, urlMap, routableParams)
        }
    }

    private def getRoutes(req: UpdateRequest, router: DocRouter, col: DocCollection,
                          urlMap: ju.Map[String, ju.List[String]],
                          routableParams: SolrParams): Try[Option[Map[String, LBClientConnection.ExtendedRequest]]] = {
        def transformReq(req: LBHttpSolrServer.Req): LBClientConnection.ExtendedRequest = req.getRequest match {
            case ur: UpdateRequest ⇒
                LBClientConnection.ExtendedRequest(Solr.SolrUpdateOperation(ur), req.getServers.asScala.toList)

            case _ ⇒ sys.error(s"${req.getRequest} isn't an UpdateRequest")
        }

        def transformRoutes(routes: ju.Map[String, LBHttpSolrServer.Req]) = {
            Option(routes) map (rs ⇒ (rs.asScala mapValues transformReq).toMap)
        }

        Try(req.getRoutes(router, col, urlMap, new ModifiableSolrParams(routableParams), config.idField)) transform
            (r ⇒ Success(transformRoutes(r)), t ⇒ Failure(Solr.InvalidRequest(t.getMessage)))
    }

    def directUpdateRoutes(zkStateReader: ZkStateReader, op: Solr.SolrUpdateOperation, clusterState: ClusterState, collection: Option[String], reqTimeout: FiniteDuration): Try[Option[DirectUpdateInfo]] = {
        val (updateRequest, allParams, routableParams) = createParams(op)

        getRoutesForRequest(zkStateReader, clusterState, updateRequest, collection, routableParams) flatMap {
            case None ⇒ Success(None)
            case Some(routes) if routes.isEmpty ⇒ Failure(Solr.InvalidRequest("No routes found for request"))
            case Some(routes) ⇒
                val routesWithTimeout = routes mapValues (r ⇒ r.copy(op = r.op withTimeout reqTimeout))
                Success(Some(DirectUpdateInfo(updateRequest, allParams, routesWithTimeout)))
        }
    }

    def getNonRoutableUpdate(updateInfo: DirectUpdateInfo, reqTimeout: FiniteDuration): Option[LBClientConnection.ExtendedRequest] = {
        val nonRoutableRequest: Option[UpdateRequest] = {
            Option(updateInfo.updateRequest.getDeleteQuery) match {
                case None ⇒ None
                case Some(qs) if qs.isEmpty ⇒ None
                case Some(qs) ⇒
                    val deleteQueryRequest = new UpdateRequest()
                    deleteQueryRequest setDeleteQuery qs
                    Some(deleteQueryRequest)
            }
        }

        val nonRoutableParams = nonRoutableParameters & updateInfo.allParams.params.keySet

        if (nonRoutableRequest.isEmpty && nonRoutableParams.isEmpty)
            None
        else {
            val request = nonRoutableRequest getOrElse new UpdateRequest()
            request setParams new ModifiableSolrParams(updateInfo.allParams)

            Some(LBClientConnection.ExtendedRequest(Solr.SolrUpdateOperation(request) withTimeout reqTimeout,
                Random.shuffle(updateInfo.routes.keySet.toList)))
        }
    }
}
