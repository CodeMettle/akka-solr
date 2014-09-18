import sbt._

object Deps {
    val akkaActor = "com.typesafe.akka" %% "akka-actor" % Versions.akka
    val akkaSlf = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
    val logback = "ch.qos.logback" % "logback-classic" % Versions.logback
    val solrj = "org.apache.solr" % "solr-solrj" % Versions.solr
    val sprayCan = "io.spray" %% "spray-can" % Versions.spray
    val sprayClient = "io.spray" %% "spray-client" % Versions.spray

    val ficus2_10 = "net.ceedubs" %% "ficus" % Versions.ficus2_10
    val ficus2_11 = "net.ceedubs" %% "ficus" % Versions.ficus2_11
}
