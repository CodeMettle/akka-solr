/*
 * TestConfigs.scala
 *
 * Updated: Oct 14, 2014
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

    def nonDefaultLbConnOpts = ConfigFactory parseString """akkasolr {
                                                           |  load-balanced-connection-defaults {
                                                           |    alive-check-interval = 5 seconds
                                                           |    non-standard-ping-limit = 10
                                                           |  }
                                                           |}""".stripMargin

    def nonDefaultCloudConnOpts = ConfigFactory parseString """akkasolr {
                                                              |  solrcloud-connection-defaults {
                                                              |    zookeeper-connect-timeout = 1 minute
                                                              |    zookeeper-client-timeout = 1 hour
                                                              |    connect-at-start = false
                                                              |    default-collection = "mycoll"
                                                              |    parallel-updates = false
                                                              |    id-field = "myid"
                                                              |  }
                                                              |}""".stripMargin

    "Configs" should "have correct defaults" in withSystem(tests = { implicit sys ⇒
        val opts = Solr.RequestOptions(sys)

        opts should equal (Solr.RequestOptions(Solr.RequestMethods.POST, Solr.SolrResponseTypes.Binary, 1.minute))

        val updOpts = Solr.UpdateOptions(sys)

        updOpts should equal (Solr.UpdateOptions(commit = false, None, overwrite = true))

        val lbConnOpts = Solr.LBConnectionOptions(sys)

        lbConnOpts should equal (Solr.LBConnectionOptions(1.minute, 5))

        val cloudConnOpts = Solr.SolrCloudConnectionOptions(sys)

        cloudConnOpts should equal(Solr
            .SolrCloudConnectionOptions(10.seconds, 10.seconds, connectAtStart = true, None, parallelUpdates = true,
            "id"))
    })

    "Default Request Options" should "be overridable" in withSystem(Some(nonDefaultReqOpts), { implicit sys ⇒
        val opts = Solr.RequestOptions(sys)

        opts should equal (Solr.RequestOptions(Solr.RequestMethods.GET, Solr.SolrResponseTypes.XML, 750.millis))
    })

    "Default Update Options" should "be overridable" in withSystem(Some(nonDefaultUpdOpts), { implicit sys ⇒
        val updOpts = Solr.UpdateOptions(sys)

        updOpts should equal (Solr.UpdateOptions(commit = true, Some(10.seconds), overwrite = false))
    })

    "Default LoadBalance connection Options" should "be overridable" in withSystem(Some(nonDefaultLbConnOpts), { implicit sys ⇒
        val connOpts = Solr.LBConnectionOptions(sys)

        connOpts should equal(Solr.LBConnectionOptions(5.seconds, 10))
    })

    "Default SolrCloud connection Options" should "be overridable" in withSystem(Some(nonDefaultCloudConnOpts), { implicit sys ⇒
        val connOpts = Solr.SolrCloudConnectionOptions(sys)

        connOpts should equal(Solr.SolrCloudConnectionOptions(1.minute, 1.hour, connectAtStart = false, Some("mycoll"),
            parallelUpdates = false, "myid"))
    })
}
