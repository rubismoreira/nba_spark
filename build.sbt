name := "nba_spark"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.9.1"

libraryDependencies += "com.lihaoyi" %% "upickle" % "3.1.0"

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.0"