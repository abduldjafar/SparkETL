import etl.bronzeDeltaLake.{FileProcessing, IngestionFromRdbms}

import etl.silverDeltaLake.RdbmsDataWarehousing
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager


object Main {
  def main(args: Array[String]): Unit = {
    val fileProcessing = FileProcessing
    val ingestionFromRdbms = IngestionFromRdbms
    val rdbmsDataWarehousing = RdbmsDataWarehousing
    val jdbcHostname = "localhost"
    val jdbcPort = 13306
    val jdbcDatabase = "employees"
    val jdbcUsername = "root"
    val jdbcPassword = "toor"

    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    val spark = SparkSession
      .builder()
      .appName("Cleansing Data")
      .config("spark.jars","jars/mariadb-java-client-3.0.7.jar")
      .config("spark.driver.extraClassPath","jars/mariadb-java-client-3.0.7.jar")
      .master("local[*]")
      .getOrCreate()

    /*
        data ingestion to bronze delta lake
     */
    //fileProcessing.process_transaction_json(
      //spark,
      //"data-lake/delta-bronze/transactions-json"
    //)

    ingestionFromRdbms.proces_employees_db(
      spark,
      "data-lake/delta-bronze",
      jdbcUrl,
      connectionProperties
    )

    rdbmsDataWarehousing.process_db_employees_from_bronze(spark, "data-lake/delta-bronze/db_employees/","delta-silver/dwh")

  }
}
