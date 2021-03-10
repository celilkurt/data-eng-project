name := "stream-processing"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= {
  Seq(
    "org.apache.flink" %% "flink-clients" % "1.10.0",
    "org.apache.flink" %% "flink-scala" % "1.10.0",
    "org.apache.flink" %% "flink-streaming-scala" % "1.10.0",
    "org.apache.kafka" % "kafka-clients" % "2.4.1",
    "org.apache.flink" %% "flink-connector-kafka" % "1.10.0",
    "ch.qos.logback"             % "logback-classic"             % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging"              % "3.9.2",
    "org.apache.flink"  % "flink-avro" % "1.12.0",
    "com.fasterxml.jackson.core"  % "jackson-databind" % "2.9.8",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.9.8",
    "com.novocode" % "junit-interface" % "0.11" % Test
  )
}



testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-s")

dependencyOverrides ++=
  Seq(
    "com.fasterxml.jackson.core"  % "jackson-databind" % "2.9.8",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.9.8"

  )


assemblyMergeStrategy in assembly := {
  case "BUILD" => MergeStrategy.discard
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case other => MergeStrategy.defaultMergeStrategy(other)
}


