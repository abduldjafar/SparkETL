package etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, when,trim}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}

import scala.io.Source

object LpeiCleansing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("Cleansing Data").
      master("local[*]").
      config("hive.metastore.uris", "thrift://192.168.15.142:9083").enableHiveSupport().getOrCreate()
    var dataColummn = spark.read.format("csv").option("delimiter"," ").load("listcolumnsLPEI")
    val dataColumns = dataColummn.select("_c0","_c1","_c2").filter("_c1 = 'n' OR _c1 = 'an'")
      .distinct().withColumnRenamed("_c0","namacolumn").drop("_c0")
      .withColumnRenamed("_c1","jenis").drop("_c1")
      .withColumnRenamed("_c2","length").drop("_c2")
      .withColumn("hivetype",
        when(col("jenis") === "an","string").otherwise("int")
      )
      .withColumn("hivetype",when(
        size(split(col("length"),",")) === 2 && col("jenis") ==="n","float")
          .when(size(split(col("length"),",")) === 1 && col("jenis") ==="n","int")
          .otherwise("string")
      )
      .select("namacolumn","hivetype")
    dataColumns.write.saveAsTable("listcolumns")

    val listcolumns = dataColumns.rdd.collect().toList


    val filename = "listTablesLPEI"

    for (filename <- Source.fromFile(filename).getLines) {
      var datas = spark.sql("select * from "+filename)
      val dataslistcolums = datas.columns.toList
      for(dataslist <- dataslistcolums){
        for(data <- listcolumns){
          if(dataslist.toString().trim == data(0).toString.trim){
            var columname = data(0).toString.trim
            var datatype = data(1).toString.trim

            if(datatype== "string"){
              datas = datas.withColumn(columname,col(columname).cast(StringType))
                .withColumn(columname,
                  when(trim(col(columname) )==="" || trim(col(columname)) ==="-" ,null)
                    .otherwise(col(columname))
                )
            } else if(datatype == "float"){
              datas = datas.withColumn(columname,col(columname).cast(FloatType))
                .withColumn(columname,
                  when(trim(col(columname) )==="",0.0)
                )

            } else if(datatype == "int"){
              datas = datas.withColumn(columname,col(columname).cast(IntegerType))
                .withColumn(columname,
                  when(trim(col(columname) )==="",0)
                )
            }else{
              datas = datas
            }
          }
        }
      }
      datas.write.saveAsTable(filename+"_hasil_cleansingx")
    }
  }
}
