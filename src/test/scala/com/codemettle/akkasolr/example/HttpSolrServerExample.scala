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

import akka.actor.ActorSystem
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.Await
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

    ultimately(system.shutdown()) {
        val config = system.settings.config.as[Config]("example")

        val conn = Await.result(Solr.Client.clientFutureTo(config.as[String]("solrAddr")), Duration.Inf)

        println(Await.result(conn ? Solr.Ping(), Duration.Inf))

        println(Await.result(conn ? (Solr.Update AddDocs Map("uuid" → UUID.randomUUID().toString, "messageType" → "DummyType") commit true), Duration.Inf))

        println(Await.result(conn ? Solr.Select(Solr.queryStringBuilder defaultField() := "DummyType"), Duration.Inf))

        println(Await.result(conn ? (Solr.Update DeleteByQuery (Solr.queryStringBuilder defaultField() := "DummyType") commit true), Duration.Inf))

        println(Await.result(conn ? Solr.Select(Solr.queryStringBuilder defaultField() := "DummyType"), Duration.Inf))
    }
}
