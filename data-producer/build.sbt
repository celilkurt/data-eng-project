name := "BootcampDataProducer"
version := "0.1"
scalaVersion := "2.13.4"

libraryDependencies ++= Seq(
  "org.scala-lang.modules"     %% "scala-parallel-collections" % "1.0.0",
  "ch.qos.logback"             % "logback-classic"             % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging"              % "3.9.2",
  "com.github.pureconfig"      %% "pureconfig"                 % "0.14.0",
  "org.apache.kafka"           % "kafka-clients"               % "2.7.0"
)
