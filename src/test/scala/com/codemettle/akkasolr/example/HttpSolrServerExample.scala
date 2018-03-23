/*
 * HttpSolrServerExample.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.example

import java.util.UUID

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.TestUtil._

import akka.actor.ActorSystem
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.control.Exception.ultimately

/**
 * @author steven
 *
 */
object HttpSolrServerExample extends App {
    val conf = """akkasolr.connectionProvider = "com.codemettle.akkasolr.ext.HttpSolrServerConnectionProvider""""
    implicit val system = ActorSystem("HSSE", ConfigFactory parseString conf withFallback ConfigFactory.load())
    import system.dispatcher
    implicit val timeout = Timeout(15.seconds)

    ultimately(system.terminate()) {
        import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

        val config = system.settings.config.as[Config]("example")

        val conn = Solr.Client.clientFutureTo(config.as[String]("solrAddr")).await

        println((conn ? Solr.Ping()).await)

        println((conn ? (Solr.Update AddDocs Map("uuid" → UUID.randomUUID().toString, "messageType" → "DummyType") commit true)).await)

        println((conn ? Solr.Select(defaultField() := "DummyType")).await)

        println((conn ? (Solr.Update DeleteByQuery (defaultField() := "DummyType") commit true)).await)

        println((conn ? Solr.Select(defaultField() := "DummyType")).await)
    }
}
