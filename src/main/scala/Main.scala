import etl.bronzeDeltaLake.{FileProcessing, IngestionFromRdbms}

import etl.silverDeltaLake.SakilaDataWarehousing
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val fileProcessing = FileProcessing
    val ingestionFromRdbms = IngestionFromRdbms
    val sakilaDataWarehousing = SakilaDataWarehousing

    

    val spark = SparkSession
      .builder()
      .appName("Cleansing Data")
      .config("jars","jars/mariadb-java-client-3.0.7.jar")
      .master("local[*]")
      .getOrCreate()
    /*
        data ingestion to bronze delta lake
     */
    fileProcessing.process_transaction_json(
      spark,
      "data-lake/delta-bronze/transactions-json"
    )
    ingestionFromRdbms.proces_sakila_db(
      spark,
      "data-lake/delta-bronze/sakila-db"
    )

    /*
        data cleansing and make it as a datawarehoue
     */
    sakilaDataWarehousing.process(spark, "data-lake/delta-bronze/transactions-json","")
    println("Process Finished")

  }
}
