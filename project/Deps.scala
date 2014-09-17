import sbt._

object Deps {
    val akkaActor = "com.typesafe.akka" %% "akka-actor" % Versions.akka
    val akkaSlf = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
    val logback = "ch.qos.logback" % "logback-classic" % Versions.logback
    val solrj = "org.apache.solr" % "solr-solrj" % Versions.solr
}
