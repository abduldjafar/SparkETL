package etl
import java.io.File
import org.apache.spark.sql.SparkSession

object Spark {
  def getListOfFiles(dir: String):List[File] = {
         val d = new File(dir)
         if (d.exists && d.isDirectory) {
               d.listFiles.filter(_.isFile).toList
          } else {
              List[File]()
          }
     }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("interfacing spark sql to hive metastore without configuration file").
      master("local[*]").
      config("hive.metastore.uris", "thrift://192.168.15.142:9083").enableHiveSupport().getOrCreate()

    val files = getListOfFiles("C:/Users/abdul/Documents/kantor/Datas/LPEI")
    for(data <- files){
      println("process data :"+data.toString)
      var dataset  = spark.read.format("csv").option("header", "true").option("delimiter", ";").
        load(data.toString)
      var x = data.toString.split("""\\""")
      var namatable = x(x.length - 1).split("\\.")(0)
      dataset.write.saveAsTable("testspark."+namatable)
      dataset.sqlContext.sql("show tables").show()

    }

  }
}
