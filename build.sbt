name := "Transformations"

version := "0.1"

scalaVersion :=  "2.11.8"

val sparkVersion = "2.3.1"
val catsVersion="1.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.typelevel" %% "cats-core" % "2.0.0-RC1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.github.pureconfig" %% "pureconfig" % "0.11.1",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "junit" % "junit" % "4.13-beta-1")