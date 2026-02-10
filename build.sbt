ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.1"
ThisBuild / organization := "com.zoomin"

Compile / run / fork := true

lazy val root = (project in file("."))
  .settings(
    name := "nostr-bigquery-pipeline",
    libraryDependencies ++= Seq(
      "org.typelevel"                 %% "cats-effect"                   % "3.5.7",
      "co.fs2"                        %% "fs2-core"                      % "3.9.4",
      "co.fs2"                        %% "fs2-io"                        % "3.9.4",
      "com.softwaremill.sttp.client3" %% "core"                          % "3.9.8",
      "com.softwaremill.sttp.client3" %% "fs2"                           % "3.9.8",
      "com.softwaremill.sttp.client3" %% "async-http-client-backend-fs2" % "3.9.8",
      "io.circe"                      %% "circe-core"                    % "0.14.15",
      "io.circe"                      %% "circe-generic"                 % "0.14.15",
      "io.circe"                      %% "circe-parser"                  % "0.14.15",
      "com.google.cloud"               % "google-cloud-bigquery"         % "2.57.1",
      "org.typelevel"                 %% "log4cats-slf4j"                % "2.7.1",
      "ch.qos.logback"                 % "logback-classic"               % "1.5.29",
      "com.github.pureconfig"         %% "pureconfig-core"               % "0.17.4",
      "com.github.pureconfig"         %% "pureconfig-cats-effect"        % "0.17.4",
      "org.scalameta"                 %% "munit"                         % "1.2.1" % Test,
      "org.typelevel"                 %% "munit-cats-effect"             % "2.1.0" % Test,
      "org.typelevel"                 %% "cats-effect-testing-scalatest" % "1.7.0" % Test
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings",
      "-language:higherKinds",
      "-language:postfixOps"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs*) => MergeStrategy.concat
      case PathList("META-INF", "maven", xs*)    => MergeStrategy.discard
      case PathList("META-INF", xs*) =>
        xs match {
          case "MANIFEST.MF" :: Nil  => MergeStrategy.discard
          case "INDEX.LIST" :: Nil   => MergeStrategy.discard
          case "DEPENDENCIES" :: Nil => MergeStrategy.discard
          case _                     => MergeStrategy.discard
        }
      case "application.conf"                  => MergeStrategy.concat
      case "reference.conf"                    => MergeStrategy.concat
      case "module-info.class"                 => MergeStrategy.discard
      case PathList("google", "protobuf", xs*) => MergeStrategy.first
      case PathList("com", "google", xs*)      => MergeStrategy.first
      case x if x.endsWith(".proto")           => MergeStrategy.first
      case _                                   => MergeStrategy.first
    },
    assembly / mainClass       := Some("com.zoomin.earth.datalake.StalkingPipeline"),
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    run / javaOptions ++= Seq(
      "-Xmx2G",
      "-Xms1G"
    )
  )
