import sbt._
import sbt.Keys._

object ZeroMQBuild extends Build {

  lazy val buildSettings = Seq(
    organization := "com.ergodicity",
    version      := "0.1-SNAPSHOT",
    scalaVersion := "2.9.1"
  )

  lazy val zeromq = Project(
    id = "zeromq",
    base = file("."),
    settings = Project.defaultSettings ++ repositoriesSetting ++ Seq(libraryDependencies ++= Dependencies.root)
  )

  // -- Settings

  override lazy val settings = super.settings ++ buildSettings

  lazy val repositoriesSetting = Seq(
    resolvers += "Sonatype Repository" at "http://oss.sonatype.org/content/groups/public/",
    resolvers += "JBoss repository" at "http://repository.jboss.org/nexus/content/repositories/",
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "Typesafe Repository ide-2.9" at "http://repo.typesafe.com/typesafe/simple/ide-2.9/",
    resolvers += "Twitter Repository" at "http://maven.twttr.com/",
    resolvers += "Akka Repository" at "http://akka.io/snapshots/"
  )
}

object Dependencies {
  import Dependency._

  val root = Seq(ostrich, scalaTime, sbinary, finagleCore, scalaSTM, slf4jApi, logback, scalaz, jodaTime, jodaConvert) ++
    Seq(Test.mockito, Test.scalatest, Test.scalacheck)
}


object Dependency {

  // Versions

  object V {
    val Scalatest    = "1.6.1"
    val Slf4j        = "1.6.4"
    val Mockito      = "1.8.1"
    val Scalacheck   = "1.9"
    val Scalaz       = "6.0.4"
    val Logback      = "1.0.0"
    val ScalaSTM     = "0.4"
    val JodaTime     = "2.0"
    val JodaConvert  = "1.2"
    val SBinary      = "0.4.0"
    val ScalaTime    = "0.5"

    // Twitter dependencies
    val Finagle      = "4.0.2"
    val Ostrich      = "7.0.0"
  }

  // Compile

  val slf4jApi          = "org.slf4j"                         % "slf4j-api"              % V.Slf4j
  val logback           = "ch.qos.logback"                    % "logback-classic"        % V.Logback
  val scalaz            = "org.scalaz"                       %% "scalaz-core"            % V.Scalaz
  val scalaSTM          = "org.scala-tools"                  %% "scala-stm"              % V.ScalaSTM
  val jodaTime          = "joda-time"                         % "joda-time"              % V.JodaTime
  val jodaConvert       = "org.joda"                          % "joda-convert"           % V.JodaConvert
  val finagleCore       = "com.twitter"                      %% "finagle-core"           % V.Finagle
  val finagleKestrel    = "com.twitter"                      %% "finagle-kestrel"        % V.Finagle
  val ostrich           = "com.twitter"                      %% "ostrich"                % V.Ostrich
  val sbinary           = "org.scala-tools.sbinary"          %% "sbinary"                % V.SBinary
  val scalaTime         = "org.scala-tools.time"             %% "time"                   % V.ScalaTime  intransitive()

  // Provided

  object Provided {

  }

  // Runtime

  object Runtime {

  }

  // Test

  object Test {
    val mockito        = "org.mockito"                 % "mockito-all"             % V.Mockito      % "test" // MIT
    val scalatest      = "org.scalatest"              %% "scalatest"               % V.Scalatest    % "test" // ApacheV2
    val scalacheck     = "org.scala-tools.testing"    %% "scalacheck"              % V.Scalacheck   % "test" // New BSD
  }
}