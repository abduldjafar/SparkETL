name := "SparkETL"

version := "0.1"

scalaVersion := "2.12.11"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
// https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake
libraryDependencies += "io.delta" %% "delta-core" % "2.0.0"
