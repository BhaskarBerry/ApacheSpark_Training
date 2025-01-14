name := "Spark_Training"

organization := "berry.training.com"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion= "3.0.1"

//autoScalaLibrary := false

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies