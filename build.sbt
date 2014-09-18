
organization := "com.github.akka-solr"

name := "akka-solr"

version := "0.0.9-SNAPSHOT"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.10.4", "2.11.2")

libraryDependencies ++= Seq(
    Deps.akkaActor,
    Deps.solrj,
    Deps.sprayCan
)

libraryDependencies ++= Seq(
    Deps.akkaSlf,
    Deps.logback
) map (_ % "test")

libraryDependencies += {
    CrossVersion partialVersion scalaVersion.value match {
        case Some((2, 10)) => Deps.ficus2_10
        case Some((2, 11)) => Deps.ficus2_11
    }
} % "test"
