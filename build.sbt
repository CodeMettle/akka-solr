
organization := "com.github.akka-scala"

name := "akka-scala"

version := "0.0.9-SNAPSHOT"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.10.4", "2.11.2")

libraryDependencies ++= Seq(
    Deps.akkaActor,
    Deps.solrj
)

libraryDependencies ++= Seq(
    Deps.akkaSlf,
    Deps.logback
) map (_ % "test")
