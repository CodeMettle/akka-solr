/*
 * TestConfigs.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._

import akka.actor.ActorSystem
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.Exception.ultimately

/**
 * @author steven
 *
 */
class TestConfigs extends FlatSpec with Matchers {
    def withSystem(conf: Option[Config] = None, tests: (ActorSystem) ⇒ Unit = _ ⇒ Unit) = {
        val baseConf = ConfigFactory.load()
        val system = ActorSystem((Random.alphanumeric take 10).mkString, conf.fold(baseConf)(_ withFallback baseConf))

        ultimately {
            system.shutdown()
            system.awaitTermination()
        } apply tests
    }

    def nonDefaultReqOpts = ConfigFactory parseString """akkasolr {
                                                        |  request-defaults {
                                                        |    method = "GET"
                                                        |    writer-type = "XML"
                                                        |    request-timeout = 750 ms
                                                        |  }
                                                        |}""".stripMargin

    def nonDefaultUpdOpts = ConfigFactory parseString """akkasolr {
                                                        |  update-defaults {
                                                        |    commit = true
                                                        |    overwrite = false
                                                        |    commit-within = 10 secs
                                                        |  }
                                                        |}""".stripMargin

    "Configs" should "have correct defaults" in withSystem(tests = { implicit sys ⇒
        val opts = Solr.RequestOptions(sys)

        opts should equal (Solr.RequestOptions(Solr.RequestMethods.POST, Solr.SolrResponseTypes.Binary, 1.minute))

        val updOpts = Solr.UpdateOptions(sys)

        updOpts should equal (Solr.UpdateOptions(commit = false, None, overwrite = true))
    })

    "Default Request Options" should "be overridable" in withSystem(Some(nonDefaultReqOpts), { implicit sys ⇒
        val opts = Solr.RequestOptions(sys)

        opts should equal (Solr.RequestOptions(Solr.RequestMethods.GET, Solr.SolrResponseTypes.XML, 750.millis))
    })

    "Default Update Options" should "be overridable" in withSystem(Some(nonDefaultUpdOpts), { implicit sys ⇒
        val updOpts = Solr.UpdateOptions(sys)

        updOpts should equal (Solr.UpdateOptions(commit = true, Some(10.seconds), overwrite = false))
    })
}
