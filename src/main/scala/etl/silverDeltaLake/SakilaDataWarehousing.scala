package etl.silverDeltaLake
import org.apache.spark.sql.SparkSession

object SakilaDataWarehousing {
  def process(spark: SparkSession, sources_delta_lake_path: String, destination_delta_lake_path: String): Unit = {
     val   data = spark.read.format("delta").load(sources_delta_lake_path)
     data.show()
  }
}
