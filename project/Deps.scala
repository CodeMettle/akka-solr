/*
 * Deps.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */

import sbt._

object Deps {
    val akkaActor = "com.typesafe.akka" %% "akka-actor" % Versions.akka
    val akkaHttpCore = "com.typesafe.akka" %% "akka-http-core" % Versions.akkaHttp
    val akkaSlf = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
    val akkaTest = "com.typesafe.akka" %% "akka-testkit" % Versions.akka
    val ficus = "com.iheart" %% "ficus" % Versions.ficus
    val jclOverSlf4j = "org.slf4j" % "jcl-over-slf4j" % Versions.slf4j
    val logback = "ch.qos.logback" % "logback-classic" % Versions.logback
    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest
    val solrj = "org.apache.solr" % "solr-solrj" % Versions.solr
}
