
organization := "com.codemettle.akka-solr"

name := "akka-solr"

version := "0.9.0-SNAPSHOT"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.10.4", "2.11.2")

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
