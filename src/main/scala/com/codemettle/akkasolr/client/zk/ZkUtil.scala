/*
 * ZkUtil.scala
 *
 * Updated: Oct 16, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
/*
package com.codemettle.akkasolr.client.zk

import java.io.IOException
import java.util.concurrent.TimeoutException

import org.apache.solr.common.SolrException
import org.apache.solr.common.cloud._
import org.apache.solr.common.util.StrUtils
import org.apache.zookeeper.KeeperException

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.Solr.SolrCloudConnectionOptions

import akka.actor.ActorRefFactory
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Random, Success, Try}

/**
 * Translates ZooKeeper-specific code from [[org.apache.solr.client.solrj.impl.CloudSolrClient]]
 *
 * @author steven
 *
 */
case class ZkUtil(config: SolrCloudConnectionOptions)(implicit arf: ActorRefFactory) {
    private def getCollectionSet(reader: ZkStateReader, clusterState: ClusterState,
                                 collection: String): Try[Set[String]] = {
        def collectionsForCollectionName(collectionName: String): Try[Set[String]] = {
            if (clusterState.getCollectionsMap.asScala contains collectionName)
                Success(Set(collectionName))
            else Option(reader.getAliases getCollectionAlias collectionName) match {
                case None => Failure(Solr.InvalidRequest(s"Collection not found: $collectionName"))
                case Some(alias) => Try(StrUtils.splitSmart(alias, ",", true).asScala.toSet)
            }
        }

        // Extract each comma separated collection name and store in a List.
        val rawCollectionsList = Try(StrUtils.splitSmart(collection, ",", true).asScala.toSet)

        rawCollectionsList flatMap (collectionNames => {
            (Try(Set.empty[String]) /: collectionNames) {
                case (f: Failure[_], _) => f
                case (Success(acc), collectionName) => collectionsForCollectionName(collectionName) map (cs => acc ++ cs)
            }
        })
    }

    private def getSlices(requestCollection: Option[String], zkStateReader: ZkStateReader,
                          clusterState: ClusterState): Try[Map[String, Slice]] = {
        def collection: Try[String] = {
            requestCollection orElse config.defaultCollection match {
                case None =>
                    Failure(Solr.InvalidRequest(
                        "No collection param specified on request and no default collection has been set"))

                case Some(c) => Success(c)
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
            (colSlices map (slice => s"${colName}_${slice.getName}" -> slice)).toMap
        }

        collectionList flatMap (collections => {
            (Try(Map.empty[String, Slice]) /: collections) {
                case (f: Failure[_], _) => f
                case (Success(acc), collectionName) =>
                    Option(clusterState getActiveSlices collectionName) map (_.asScala) match {
                        case None => Failure(Solr.InvalidRequest(s"Could not find collection: $collectionName"))
                        case Some(colSlices) => Success(acc ++ mapColSlices(collectionName, colSlices))
                    }
            }
        })
    }

    def connect(zkHost: String): Future[ZkStateReader] = {
        implicit val dispatcher = Solr.Client.zookeeperDispatcher

        Future {
            val zk = new ZkStateReader(zkHost, config.clientTimeoutMS, config.connectTimeoutMS)
            zk.createClusterStateWatchersAndUpdate()
            zk
        } transform(identity, {
            case zke: ZooKeeperException => zke
            case e@(_: InterruptedException | _: KeeperException | _: IOException | _: TimeoutException) =>
                new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e)
            case t => t
        })
    }

    def getUrlsForNormalRequest(isUpdateRequest: Boolean, requestCollection: Option[String],
                                zkStateReader: ZkStateReader): Try[Vector[String]] = {
        val clusterState = zkStateReader.getClusterState

        // took a while to decipher this code in CloudSolrServer, but what it's doing is getting all the nodes
        // and randomly picking the order for queries, but for updates it's putting the shuffled list of leaders
        // into the list before the shuffled list of replicas, so leaders will be tried first
        def createUrlList(slices: Map[String, Slice]): Vector[String] = {

            val liveNodes = clusterState.getLiveNodes.asScala.toSet

            def extractUrlsFromSlice(leaders: Vector[String], replicas: Vector[String], nodes: Set[String],
                                     slice: Slice): (Vector[String], Vector[String], Set[String]) = {
                ((leaders, replicas, nodes) /: slice.getReplicasMap.values().asScala) {
                    case ((leaderAcc, replicaAcc, nodesAcc), replica) =>
                        val coreNodeProps = new ZkCoreNodeProps(replica)
                        val node = coreNodeProps.getNodeName
                        if (!(liveNodes contains node) ||
                            (Replica.State.getState(coreNodeProps.getState) != Replica.State.ACTIVE) ||
                            nodesAcc(node)) {
                            (leaderAcc, replicaAcc, nodesAcc)
                        } else {
                            val newNodes = nodesAcc + node

                            if (coreNodeProps.isLeader)
                                (leaderAcc :+ coreNodeProps.getCoreUrl, replicaAcc, newNodes)
                            else
                                (leaderAcc, replicaAcc :+ coreNodeProps.getCoreUrl, newNodes)
                        }
                }
            }

            val (leaders, replicas, _) = ((Vector.empty[String], Vector.empty[String], Set.empty[String]) /:
                slices.values) {
                case ((leadAcc, replAcc, nodesAcc), slice) => extractUrlsFromSlice(leadAcc, replAcc, nodesAcc, slice)
            }

            if (isUpdateRequest)
                Random.shuffle(leaders) ++ Random.shuffle(replicas)
            else
                Random.shuffle(leaders ++ replicas)
        }

        getSlices(requestCollection, zkStateReader, clusterState) map createUrlList
    }
}
*/
