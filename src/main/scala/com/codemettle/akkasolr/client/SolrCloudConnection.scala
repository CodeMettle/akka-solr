/*
 * SolrCloudConnection.scala
 *
 * Updated: Oct 13, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.client

import java.io.IOException
import java.util.concurrent.TimeoutException

import org.apache.solr.common.SolrException
import org.apache.solr.common.cloud._
import org.apache.zookeeper.KeeperException

import com.codemettle.akkasolr.Solr

import akka.actor.{Actor, ActorLogging, ActorRef}
import scala.concurrent.Future
import scala.util.{Failure, Try}

/**
 * @author steven
 *
 */
class SolrCloudConnection(lbServer: ActorRef, zkHost: String, config: Solr.SolrCloudConnectionOptions) extends Actor with ActorLogging {
    private var zkStateReaderOpt = Option.empty[ZkStateReader]

    override def postStop() = {
        super.postStop()

        Try(zkStateReaderOpt foreach (_.close())) match {
            case Failure(t) ⇒ log.error(t, "Error closing ZkStateReader")
            case _ ⇒
        }
    }
def receive = Actor.emptyBehavior
    private def zkStateReader: Future[ZkStateReader] = {
        zkStateReaderOpt match {
            case Some(zk) ⇒ Future successful zk
            case None ⇒
                implicit val dispatcher = Solr.Client.zookeeperDispatcher

                Future {
                    val zk = new ZkStateReader(zkHost, config.clientTimeoutMS, config.connectTimeoutMS)
                    zk.createClusterStateWatchersAndUpdate()
                    zk
                } transform (identity, {
                    case zke: ZooKeeperException ⇒ zke
                    case e @ (_: InterruptedException | _: KeeperException | _: IOException | _: TimeoutException) ⇒
                        new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e)
                    case t ⇒ t
                })
        }
    }
}
