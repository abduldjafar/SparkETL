import etl.bronzeDeltaLake.{FileProcessing, IngestionFromRdbms}
import etl.silverDeltaLake.RdbmsDataWarehousing
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import util.Properties.envOrElse
import java.util.Properties
import com.typesafe.config.{Config => TConfig, ConfigFactory}
import java.io.File
import config.Config



object Main {

  
  def main(args: Array[String]): Unit = {

    var filepath: String = "s3://kotekaman-dev/config/application.conf"

    if (args.length == 1) {
         filepath = args(0).toString
    }
    
    val applicationConf: TConfig = Config(filepath)


    val fileProcessing = FileProcessing
    val ingestionFromRdbms = IngestionFromRdbms
    val rdbmsDataWarehousing = RdbmsDataWarehousing
 
    val delta_lake_path = "s3a://kotekaman-dev/"


    val connectionProperties = new Properties()

    

    val spark = SparkSession
      .builder()
      .appName("Cleansing Data")
      .config("spark.jars", "jars/mariadb-java-client-3.0.7.jar")
      .config(
        "spark.driver.extraClassPath",
        "jars/mariadb-java-client-3.0.7.jar"
      )
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
      .config("spark.hadoop.fs.s3a.access.key", applicationConf.getString("aws.access_key_id"))
      .config(
        "spark.hadoop.fs.s3a.secret.key",
        applicationConf.getString("aws.secret_access_key")
      )
      .master("local[*]")
      .getOrCreate()

    ingestionFromRdbms.proces_employees_db(
      spark,
      delta_lake_path.concat("data-lake/delta-bronze"),
      connectionProperties,
      filepath
    )

    rdbmsDataWarehousing.process_db_employees_from_bronze(
      spark,
      delta_lake_path.concat("data-lake/delta-bronze/db_employees/"),
      delta_lake_path.concat("data-lake/delta-silver/dwh/")
    )


     /*
        data ingestion to bronze delta lake
     */
    //fileProcessing.process_transaction_json(
    //spark,
    //"data-lake/delta-bronze/transactions-json"
    //)

  }
}
