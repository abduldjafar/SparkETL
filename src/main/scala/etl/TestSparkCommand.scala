package etl

import org.apache.spark.sql.SparkSession

object TestSparkCommand {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
      appName("TestSparkCommand").
      master("local").
      config("hive.metastore.uris", "thrift://192.168.15.142:9083").enableHiveSupport().getOrCreate()
    val dataframe = spark.sql("select * from nib_hasil_cleansingx")
    dataframe.show()
  }
}
