name := "ExecuteQuery"

version := "0.1"
scalaVersion := "2.11.12"
idePackagePrefix := Some("org.novakorp.io")
val sparkVersion = "2.4.0"
mainClass := Some("org.novakorp.com.entry")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion
  ,"org.apache.spark" %% "spark-sql" % sparkVersion
  ,"org.apache.spark" %% "spark-yarn" % sparkVersion
  ,"org.json4s" %% "json4s-jackson" % "3.5.0")
