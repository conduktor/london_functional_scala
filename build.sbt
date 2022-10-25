ThisBuild / scalaVersion     := "2.13.9"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"
ThisBuild / scalacOptions ++= Seq("-Ywarn-unused")
inThisBuild(
  List(
    scalaVersion      := "2.13.9",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
  )
)

addCommandAlias("fix", "; all compile:scalafix test:scalafix; all scalafmtSbt scalafmtAll")

val zioVersion = "2.0.2"

val circeVersion = "0.14.3"

lazy val root = (project in file("."))
  .settings(
    name := "functional_programming_talk",
    libraryDependencies ++= Seq(
      "dev.zio"                       %% "zio"                        % zioVersion,
      "dev.zio"                       %% "zio-interop-cats"           % "3.3.0",
      "dev.zio"                       %% "zio-kafka"                  % "2.0.1",
      "dev.zio"                       %% "zio-test"                   % zioVersion   % Test,
      "dev.zio"                       %% "zio-test-sbt"               % zioVersion   % Test,
      "com.dimafeng"                  %% "testcontainers-scala-kafka" % "0.40.11"    % Test,
      "com.softwaremill.sttp.client3" %% "zio"                        % "3.8.3"      % Test,
      "com.softwaremill.sttp.client3" %% "circe"                      % "3.8.3"      % Test,
      "com.softwaremill.sttp.tapir"   %% "tapir-zio"                  % "1.1.3",
      "com.softwaremill.sttp.tapir"   %% "tapir-http4s-server-zio"    % "1.1.3",
      "com.softwaremill.sttp.tapir"   %% "tapir-http4s-server"        % "1.1.3",
      "org.http4s"                    %% "http4s-blaze-server"        % "0.23.12",
      "com.softwaremill.sttp.tapir"   %% "tapir-json-circe"           % "1.1.3",
      "io.circe"                      %% "circe-core"                 % circeVersion,
      "io.circe"                      %% "circe-generic"              % circeVersion,
      "io.circe"                      %% "circe-generic-extras"       % circeVersion,
      "ch.qos.logback"                 % "logback-classic"            % "1.4.4",
      "io.circe"                      %% "circe-literal"              % circeVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )
