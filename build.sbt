import SonatypeKeys._

// Metadata

organization := "com.codemettle.akka-solr"

name := "akka-solr"

version := "0.9.1-SNAPSHOT"

description := "Solr HTTP client using Akka and Spray"

startYear := Some(2014)

homepage := Some(url("https://github.com/CodeMettle/akka-solr"))

organizationName := "CodeMettle, LLC"

organizationHomepage := Some(url("http://www.codemettle.com"))

licenses += ("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scmInfo := Some(
    ScmInfo(url("https://github.com/CodeMettle/akka-solr"), "scm:git:https://github.com/CodeMettle/akka-solr.git",
        Some("scm:git:git@github.com:CodeMettle/akka-solr.git")))

pomExtra := {
    <developers>
        <developer>
            <name>Steven Scott</name>
            <email>steven@codemettle.com</email>
            <url>https://github.com/codingismy11to7/</url>
        </developer>
    </developers>
}

// Build

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.10.4", "2.11.2")

scalacOptions ++= Seq("-unchecked", "-feature", "-deprecation")

libraryDependencies ++= Seq(
    Deps.akkaActor,
    Deps.solrj % "provided",
    Deps.sprayCan
)

libraryDependencies ++= Seq(
    Deps.akkaSlf,
    Deps.akkaTest,
    Deps.solrj, // explicitly include even though not technically needed
    Deps.jclOverSlf4j,
    Deps.guava,
    Deps.logback,
    Deps.scalaTest
) map (_ % "test")

libraryDependencies += {
    CrossVersion partialVersion scalaVersion.value match {
        case Some((2, 10)) => Deps.ficus2_10
        case Some((2, 11)) => Deps.ficus2_11
    }
} % "test"

publishArtifact in Test := true

autoAPIMappings := true

apiMappings ++= {
    val cp: Seq[Attributed[File]] = (fullClasspath in Compile).value
    def findManagedDependency(organization: String, name: String): File = {
        ( for {
            entry <- cp
            module <- entry.get(moduleID.key)
            if module.organization == organization
            if module.name.startsWith(name)
            jarFile = entry.data
        } yield jarFile
            ).head
    }
    Map(
        findManagedDependency("org.scala-lang", "scala-library") -> url(s"http://www.scala-lang.org/api/${scalaVersion.value}/"),
        findManagedDependency("com.typesafe.akka", "akka-actor") -> url(s"http://doc.akka.io/api/akka/${Versions.akka}/")
    )
}

// Publish

xerial.sbt.Sonatype.sonatypeSettings

profileName := "com.codemettle"
