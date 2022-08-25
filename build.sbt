name := "SparkETL"

version := "0.1"

scalaVersion := "2.12.11"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
// https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake
libraryDependencies += "io.delta" %% "delta-core" % "2.0.0"
// https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client
libraryDependencies += "org.mariadb.jdbc" % "mariadb-java-client" % "3.0.7"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.16"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.4"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.2" % "provided"
// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies ++= Seq(

    "com.typesafe" % "config" % "1.3.3"
  
)
// https://mvnrepository.com/artifact/com.amazon.deequ/deequ
libraryDependencies += "com.amazon.deequ" % "deequ" % "2.0.1-spark-3.2"


assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

