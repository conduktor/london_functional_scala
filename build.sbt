ThisBuild / scalaVersion := "2.13.9"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"

val zioVersion = "2.0.2"

val circeVersion = "0.14.3"

lazy val root = (project in file("."))
  .settings(
    name := "functional_programming_talk",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-kafka" % "2.0.0",
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-kafka" % "0.40.10" % Test,
      "ch.qos.logback" % "logback-classic" % "1.4.1",
      "com.softwaremill.sttp.tapir" %% "tapir-zio" % "1.1.1",
      "com.softwaremill.sttp.tapir" %% "tapir-zio-http-server" % "1.1.1",
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % "1.1.1",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-literal" % circeVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
