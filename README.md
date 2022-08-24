# SparkETL

## Description
Self project for implementing delta lake architecture


## Architecture
![modern_sata_stack_2_new](https://user-images.githubusercontent.com/26897306/185607570-a4908b05-99c5-4dd5-9bb5-5990932d83d1.jpg)

## Notes
1. If want to trying in local developemnt without run in spark cluster please install external dependencies use sbt file

## Build Fat Jar
`sbt assembly`

## build thin jar
`sbt package`

## Run use Fat jar (spark-submit)
`./spark-submit --packages io.delta:delta-core_2.12:2.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" SparkETL-assembly-0.1.jar`
