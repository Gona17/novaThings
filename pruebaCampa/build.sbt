name := "PruebaCampaniadorQualia"

version := "0.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.4.0"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion
  ,"org.apache.spark" %% "spark-sql" % sparkVersion
  ,"org.scalatest" %% "scalatest" % "3.0.1" % Test
  ,"org.json4s" %% "json4s-native" % "3.5.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7")