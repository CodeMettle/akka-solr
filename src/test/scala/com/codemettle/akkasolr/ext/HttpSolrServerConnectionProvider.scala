/*
 * HttpSolrServerConnectionProvider.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import org.apache.solr.client.solrj.impl.HttpSolrServer
import spray.http.Uri

import com.codemettle.akkasolr.client.SolrServerClientConnection

import akka.actor.Props

/**
 * @author steven
 *
 */
class HttpSolrServerConnectionProvider extends ConnectionProvider {
    override def connectionActorProps(uri: Uri): Props = {
        SolrServerClientConnection props new HttpSolrServer(uri.toString())
    }
}
