import Dependencies._

ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "0.0.2-SNAPSHOT"
ThisBuild / organization     := "com.aerospike"
ThisBuild / organizationName := "Aerospike"
ThisBuild / organizationHomepage := Some(url("https://aerospike.com"))
ThisBuild / description      := "Aerospike KV Store for Chronon"
ThisBuild / homepage         := Some(url("https://github.com/aerospike/aerospike-chronon"))
ThisBuild / licenses         := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / scmInfo          := Some(ScmInfo(
  url("https://github.com/aerospike/aerospike-chronon"),
  "scm:git:git@github.com:aerospike/aerospike-chronon.git"
))
ThisBuild / developers       := List(
  Developer("aerospike", "Aerospike, Inc.", "", url("https://aerospike.com"))
)

lazy val pomProperties = taskKey[(File, String)]("META-INF/maven pom.properties entry for the published jars")

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "aerospike-chronon",
    Compile / doc / scalacOptions ++= Seq(
      "-doc-title", "aerospike-chronon",
      "-doc-version", version.value,
      "-groups",
      "-implicits"
    ),
    // Chronon and slf4j are provided by the host Chronon JVM at runtime; they
    // stay visible on the test/integration classpaths. logback is tests only —
    // the host supplies the logging engine.
    libraryDependencies ++= Seq(
      chrononApi % "provided;test;it",
      chrononOnline % "provided;test;it",
      aerospikeClient,
      scallop,
      sparkSql % "provided;test;it",
      logback % "test,it",
      slf4j % "provided;test;it",
      scalatest % "test,it"
    ),
    crossScalaVersions := Seq("2.12.18", "2.13.12"),
    // Online jar loaded by Chronon's Driver: the loader is the entry point,
    // the Scala library and Chronon/Spark/logback come from the host.
    assembly / mainClass := Some("com.aerospike.chronon.Spark2AerospikeLoader"),
    assembly / test := {},
    assembly / assemblyPackageScala / assembleArtifact := false,
    // The JFrog deploy (aerospike/shared-workflows) reads coordinates from the first
    // pom.properties in a jar, so the assembly must not carry its dependencies' copies;
    // it is published with a sibling POM instead.
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "maven", _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    // sbt does not write pom.properties; the JFrog deploy needs it to place the
    // -sources and -javadoc jars under com/aerospike/<artifactId>/<version>/.
    pomProperties := {
      val artifactId = s"${moduleName.value}_${scalaBinaryVersion.value}"
      val file = crossTarget.value / "pom.properties"
      IO.write(file, s"groupId=${organization.value}\nartifactId=$artifactId\nversion=${version.value}\n")
      file -> s"META-INF/maven/${organization.value}/$artifactId/pom.properties"
    },
    Compile / packageBin / mappings += pomProperties.value,
    Compile / packageSrc / mappings += pomProperties.value,
    Compile / packageDoc / mappings += pomProperties.value,
    // avro-mapred's pom declares a hadoop2-classifier artifact that is not on Central
    excludeDependencies ++= Seq(
      ExclusionRule("org.apache.avro", "avro-mapred")
    ),
    Test / fork := true,
    IntegrationTest / fork := true,
    IntegrationTest / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "-Dnetty.tryReflectionSetAccessible=true"
    ),
    // chronon 0.0.115 jars are Java 17+; Spark 3.5 needs these opens on JDK 17+
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "-Dnetty.tryReflectionSetAccessible=true"
    )
  )
