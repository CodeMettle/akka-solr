/*
 * HttpSolrServerConnectionProvider.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import org.apache.solr.client.solrj.impl.HttpSolrClient
import spray.http.Uri

import com.codemettle.akkasolr.client.SolrServerClientConnection

import akka.actor.{ExtendedActorSystem, Props}

/**
 * @author steven
 *
 */
class HttpSolrServerConnectionProvider extends ConnectionProvider {
    override def connectionActorProps(uri: Uri, system: ExtendedActorSystem): Props = {
        SolrServerClientConnection props new HttpSolrClient(uri.toString())
    }
}
