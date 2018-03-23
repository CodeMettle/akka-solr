import SonatypeKeys._

// Metadata

organization := "com.codemettle.akka-solr"

name := "akka-solr"

description := "Solr HTTP client using Akka and Spray"

startYear := Some(2014)

homepage := Some(url("https://github.com/CodeMettle/akka-solr"))

organizationName := "CodeMettle, LLC"

organizationHomepage := Some(url("http://www.codemettle.com"))

licenses += ("Apache License, Version 2.0" → url("http://www.apache.org/licenses/LICENSE-2.0.html"))

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
        <developer>
            <name>Cliff Ford</name>
            <email>cliff@codemettle.com</email>
            <url>https://github.com/compfix/</url>
        </developer>
    </developers>
}

// Build

crossScalaVersions := Seq("2.11.11", "2.12.5")

scalaVersion := crossScalaVersions.value.head

scalacOptions ++= Seq("-unchecked", "-feature", "-deprecation")

libraryDependencies ++= Seq(
    Deps.akkaActor,
    Deps.akkaStream,
    Deps.solrj % Provided,
    Deps.akkaHttpCore
)

libraryDependencies ++= Seq(
    Deps.akkaSlf,
    Deps.akkaTest,
    Deps.ficus,
    Deps.solrj, // explicitly include even though not technically needed
    Deps.jclOverSlf4j,
    Deps.logback,
    Deps.scalaTest
) map (_ % Test)

scalacOptions += {
    CrossVersion partialVersion scalaVersion.value match {
        case Some((x, y)) if x >= 2 && y >= 12 ⇒ "-target:jvm-1.8"
        case _ ⇒ "-target:jvm-1.6"
    }
}

publishArtifact in Test := true

autoAPIMappings := true

apiMappings ++= {
    val cp: Seq[Attributed[File]] = (fullClasspath in Compile).value
    def findManagedDependency(moduleId: ModuleID): File = {
        ( for {
            entry ← cp
            module ← entry.get(moduleID.key)
            if module.organization == moduleId.organization
            if module.name startsWith moduleId.name
            jarFile = entry.data
        } yield jarFile
        ).head
    }
    Map(
        findManagedDependency("org.scala-lang" % "scala-library" % scalaVersion.value) -> url(s"http://www.scala-lang.org/api/${scalaVersion.value}/"),
        findManagedDependency(Deps.akkaActor) -> url(s"http://doc.akka.io/api/akka/${Versions.akka}/"),
        findManagedDependency(Deps.solrj) -> url(s"https://lucene.apache.org/solr/${Versions.solr.replace('.', '_')}/solr-solrj/")
    )
}

// internal release

//publishMavenStyle := true
//
//publishTo := Some(Resolver.ssh("CodeMettle Maven", "maven.codemettle.com",
//    s"archiva/data/repositories/${if (isSnapshot.value) "snapshots" else "internal"}/") as
//    ("archiva", Path.userHome / ".ssh" / "id_rsa"))


// Release

releasePublishArtifactsAction := PgpKeys.publishSigned.value
releaseCrossBuild := true

// Publish

xerial.sbt.Sonatype.sonatypeSettings

profileName := "com.codemettle"
