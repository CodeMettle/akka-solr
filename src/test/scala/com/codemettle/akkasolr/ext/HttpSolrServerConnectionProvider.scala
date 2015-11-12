/*
 * HttpSolrServerConnectionProvider.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import org.apache.http.impl.client.SystemDefaultHttpClient
import org.apache.solr.client.solrj.impl.{HttpClientUtil, HttpSolrServer}
import org.apache.solr.common.params.ModifiableSolrParams
import spray.http.Uri

import com.codemettle.akkasolr.client.SolrServerClientConnection

import akka.actor.{ExtendedActorSystem, Props}
import akka.event.Logging

/**
 * @author steven
 *
 */
class HttpSolrServerConnectionProvider extends ConnectionProvider {
    override def connectionActorProps(uri: Uri, username: Option[String], password: Option[String],
                                      system: ExtendedActorSystem): Props = {
        def httpSolrServer = {
            def clientOpt = for (u ← username; p ← password) yield {
                val params = new ModifiableSolrParams
                params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128)
                params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32)
                params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false)

                Logging(system, getClass).info(s"Creating new http client, config:$params")

                val client = new SystemDefaultHttpClient
                HttpClientUtil.configureClient(client, params)

                params.clear()

                params.set(HttpClientUtil.PROP_BASIC_AUTH_USER, u)
                params.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, p)

                HttpClientUtil.configureClient(client, params)

                client
            }

            new HttpSolrServer(uri.toString(), clientOpt.orNull)
        }

        SolrServerClientConnection props httpSolrServer
    }
}
