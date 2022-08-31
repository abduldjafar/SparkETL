sbt package
spark-submit --packages com.github.housepower:clickhouse-spark-runtime-3.3_2.12:0.5.0,org.scala-lang:scala-reflect:2.12.10,io.delta:delta-core_2.12:2.0.0,org.apache.hadoop:hadoop-aws:3.2.4,com.typesafe:config:1.3.3,com.amazon.deequ:deequ:2.0.1-spark-3.2  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" target/scala-2.12/sparketl_2.12-0.1.jar s3://kotekaman-dev/config/application.conf s3a://kotekaman-dev/ s3a://kotekaman-dev/data-sources/
#org.apache.httpcomponents:httpclient:4.5.13