/*
 * SolrCloudConnection.scala
 *
 * Updated: Oct 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.client

import org.apache.solr.client.solrj.impl.CloudSolrClient

import com.codemettle.akkasolr.Solr

import akka.actor._

/**
 * @author steven
 *
 */
object SolrCloudConnection {
    def props(zkHost: String, config: Solr.SolrCloudConnectionOptions) =
        Props(new SolrCloudConnection(zkHost, config))
}

class SolrCloudConnection(zkHost: String, config: Solr.SolrCloudConnectionOptions)
    extends Actor with ActorLogging {

    private val solrClient = {
        val client = new CloudSolrClient.Builder()
          .withZkHost(zkHost)
          .withParallelUpdates(config.parallelUpdates)
          .build()

        client.setZkConnectTimeout(config.connectTimeoutMS)
        client.setZkClientTimeout(config.clientTimeoutMS)
        config.defaultCollection.foreach(client.setDefaultCollection)
        client.setIdField(config.idField)

        client
    }

    private val sscc = context.actorOf(SolrServerClientConnection.props(solrClient), "sscc")

    override def receive: Receive = {
        case req => sscc forward req
    }
}
