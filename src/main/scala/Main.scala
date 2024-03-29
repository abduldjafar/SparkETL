import etl.bronzeDeltaLake.{FileProcessing, IngestionFromRdbms}
import etl.silverDeltaLake.RdbmsDataWarehousing
import dqc.BronzeDeltaLakeDqc
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import util.Properties.envOrElse
import java.util.Properties
import com.typesafe.config.{Config => TConfig, ConfigFactory}
import java.io.File
import config.Config
import java.io.OutputStream; 
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;



object Main {

  
  def main(args: Array[String]): Unit = {

    var filepath: String = ""
    var delta_lake_path: String = ""
    var s3_data_sources: String = ""

    if (args.length == 3) {
         filepath = args(0).toString
         delta_lake_path = args(1).toString
         s3_data_sources = args(2).toString
    }else{
      filepath = "s3://kotekaman-dev/config/application.conf"
      delta_lake_path = "s3a://kotekaman-dev/"
      s3_data_sources = "s3a://kotekaman-dev/data-sources/"
    }

    
    
    val applicationConf: TConfig = Config(filepath)


    val fileProcessing = FileProcessing
    val ingestionFromRdbms = IngestionFromRdbms
    val rdbmsDataWarehousing = RdbmsDataWarehousing

    val bronzeDqc = BronzeDeltaLakeDqc
 


    val connectionProperties = new Properties()

    

    val spark = SparkSession
      .builder()
      .appName("Cleansing Data")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
      .config("spark.hadoop.fs.s3a.access.key", applicationConf.getString("aws.access_key_id"))
      .config(
        "spark.hadoop.fs.s3a.secret.key",
        applicationConf.getString("aws.secret_access_key")
      )
      .getOrCreate()

    fileProcessing.process_airbnb(spark,s3_data_sources,delta_lake_path.concat("data-lake/delta-bronze/airbnn_example_datas"))

  }
}
