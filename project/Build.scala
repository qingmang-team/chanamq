import sbt._
import sbt.Keys._

object Build extends sbt.Build {

  lazy val root = Project("chana-mq", file("."))
    .aggregate(chana_mq_base, chana_mq_server, chana_mq_test)
    .settings(basicSettings: _*)
    .settings(Formatting.buildFileSettings: _*)
    .settings(noPublishing: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka)

  lazy val chana_mq_base = Project("chana-mq-base", file("chana-mq-base"))
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val chana_mq_server = Project("chana-mq-server", file("chana-mq-server"))
    .dependsOn(chana_mq_base)
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka ++ Dependencies.akka_http ++ Dependencies.cassandra_driver)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(Packaging.settings)
    .settings(
      mainClass in Compile := Some("chana.mq.amqp.server.AMQPServer")
    )

  lazy val chana_mq_test = Project("chana-mq-test", file("chana-mq-test"))
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka ++ Dependencies.akka_http ++ Dependencies.rabbitmq_client)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(Packaging.settings)

  lazy val basicSettings = Defaults.coreDefaultSettings ++ Seq(
    organization := "chana.io",
    version := "0.1.0",
    resolvers ++= Seq(
      "Local Maven" at Path.userHome.asURL + ".m2/repository",
      "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/",
      "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    ),
    fork in run := true,
    fork in Test := true,
    parallelExecution in Test := false,
    scalaVersion := "2.12.2",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    //javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),  // TODO options make javadoc fail
    credentials += Credentials(Path.userHome / ".ivy2" / ".wdj_credentials")
  ) ++ Environment.settings ++ Formatting.settings

  lazy val noPublishing = Seq(
    publish := (),
    publishLocal := (),
    publishTo := None
  )
}

object Dependencies {

  private val AKKA_VERSION = "2.5.3"
  private val AKKA_HTTP_VERSION = "10.0.8"
  private val SLF4J_VERSION = "1.7.24"

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-remote" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-cluster-sharding" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-cluster-tools" % AKKA_VERSION,
    //"com.typesafe.akka" %% "akka-contrib" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-persistence" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-stream" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-slf4j" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION % Test,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % AKKA_VERSION % Test,
    "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.50-M2" % Runtime,
    "org.iq80.leveldb" % "leveldb" % "0.9" % Runtime,
    "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % Runtime
  )

  val akka_http = Seq(
    "com.typesafe.akka" %% "akka-http-core" % AKKA_HTTP_VERSION,
    "com.typesafe.akka" %% "akka-http" % AKKA_HTTP_VERSION,
    "com.typesafe.akka" %% "akka-http-spray-json" % AKKA_HTTP_VERSION
  )

  val log = Seq(
    "org.slf4j" % "slf4j-api" % SLF4J_VERSION,
    "org.slf4j" % "jcl-over-slf4j" % SLF4J_VERSION,
    "org.slf4j" % "log4j-over-slf4j" % SLF4J_VERSION,
    "ch.qos.logback" % "logback-classic" % "1.2.1"
  )

  val test = Seq(
    "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % Test,
    "org.scalatest" %% "scalatest" % "3.0.1" % Test
  )

  val rabbitmq_client = Seq(
    "com.rabbitmq" % "amqp-client" % "4.1.0"
  )

  val cassandra_driver = Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0",
    "com.datastax.cassandra" % "cassandra-driver-extras" % "3.2.0",
    "org.xerial.snappy" % "snappy-java" % "1.1.4"
  )

  val basic: Seq[ModuleID] = log ++ test
}

object Environment {
  object BuildEnv extends Enumeration {
    val Production, Stage, Test, Developement = Value
  }
  val buildEnv = settingKey[BuildEnv.Value]("The current build environment")

  val settings = Seq(
    buildEnv := {
      sys.props.get("env")
        .orElse(sys.env.get("BUILD_ENV"))
        .flatMap {
          case "prod"  => Some(BuildEnv.Production)
          case "stage" => Some(BuildEnv.Stage)
          case "test"  => Some(BuildEnv.Test)
          case "dev"   => Some(BuildEnv.Developement)
          case _       => None
        }
        .getOrElse(BuildEnv.Developement)
    },
    onLoadMessage := {
      // old message as well
      val defaultMessage = onLoadMessage.value
      val env = buildEnv.value
      s"""|$defaultMessage
          |Working in build environment: $env""".stripMargin
    }
  )
}

object Formatting {
  import com.typesafe.sbt.SbtScalariform
  import com.typesafe.sbt.SbtScalariform.ScalariformKeys
  import ScalariformKeys._

  val BuildConfig = config("build") extend Compile
  val BuildSbtConfig = config("buildsbt") extend Compile

  // invoke: build:scalariformFormat
  val buildFileSettings: Seq[Setting[_]] = SbtScalariform.noConfigScalariformSettings ++
    inConfig(BuildConfig)(SbtScalariform.configScalariformSettings) ++
    inConfig(BuildSbtConfig)(SbtScalariform.configScalariformSettings) ++ Seq(
      scalaSource in BuildConfig := baseDirectory.value / "project",
      scalaSource in BuildSbtConfig := baseDirectory.value,
      includeFilter in (BuildConfig, format) := ("*.scala": FileFilter),
      includeFilter in (BuildSbtConfig, format) := ("*.sbt": FileFilter),
      format in BuildConfig := {
        val x = (format in BuildSbtConfig).value
        (format in BuildConfig).value
      },
      ScalariformKeys.preferences in BuildConfig := formattingPreferences,
      ScalariformKeys.preferences in BuildSbtConfig := formattingPreferences
    )

  val settings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test := formattingPreferences
  )

  val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(SpacesAroundMultiImports, true)
      .setPreference(IndentSpaces, 2)
  }
}

object Packaging {
  // Good example https://github.com/typesafehub/activator/blob/master/project/Packaging.scala
  import com.typesafe.sbt.SbtNativePackager._
  import com.typesafe.sbt.packager.Keys._
  import com.typesafe.sbt.packager.archetypes._

  // This is dirty, but play has stolen our keys, and we must mimc them here.
  val stage = TaskKey[File]("stage")
  val dist = TaskKey[File]("dist")

  import Environment.{ BuildEnv, buildEnv }
  val settings = packageArchetype.java_application ++ Seq(
    name in Universal := s"${name.value}",
    dist <<= packageBin in Universal,
    mappings in Universal += {
      val confFile = buildEnv.value match {
        case BuildEnv.Developement => "dev.conf"
        case BuildEnv.Test         => "test.conf"
        case BuildEnv.Stage        => "stage.conf"
        case BuildEnv.Production   => "prod.conf"
      }
      (sourceDirectory(_ / "universal" / "conf").value / confFile) -> "conf/application.conf"
    }
  )
}

