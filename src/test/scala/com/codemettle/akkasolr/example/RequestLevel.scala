/*
 * RequestLevel.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.example

import com.typesafe.config.Config
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
object RequestLevel extends App {
    implicit val system = ActorSystem("ReqLev")
    implicit val timeout = Timeout(15.seconds)

    val config = system.settings.config.as[Config]("example")

    ultimately(system.shutdown()) {
        import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._
        import spray.util._

        val req = Solr.Select(rawQuery(config.as[String]("testQuery")))

        println((Solr.Client.manager ? Solr.Request(config.as[String]("solrAddr"), req)).await)
    }
}
