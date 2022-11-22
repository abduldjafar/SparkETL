package etl.bronzeDeltaLake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{
  IntegerType,
  DoubleType,
  BooleanType,
  LongType,
  StructType,
  ArrayType
}
import org.apache.spark.sql.functions.{
  explode,
  col,
  avg,
  sum,
  desc,
  min,
  max,
  slice,
  trim,
  ltrim,
  rtrim
}

object FileProcessing {
  def flattenStructSchema(
      schema: StructType,
      prefix: String = null
  ): Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ =>
          Array(
            col(columnName).as(columnName.replace(".", "_").replace("$", ""))
          )
      }

    })
  }

  def process_airbnb(
      spark: SparkSession,
      data_source: String,
      delta_lake_path: String
  ): Unit = {

    var data_frame = spark.read.json(
      data_source.concat("sample_airbnb/listingsAndReviews.json")
    )

    val df3 = data_frame
      .select(flattenStructSchema(data_frame.schema): _*)
      .withColumn("reviews", explode(col("reviews")))
      .withColumn(
        "host_host_verifications",
        explode(col("host_host_verifications"))
      )
      .withColumn("amenities", explode(col("amenities")))
      .withColumn(
        "address_location_coordinates",
        explode(col("address_location_coordinates"))
      )

    val cleanned_datas = df3.select(flattenStructSchema(df3.schema): _*)

    cleanned_datas.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(delta_lake_path.concat("/tb_main"))
  }
  def process_transaction_json(
      spark: SparkSession,
      delta_lake_path: String
  ): Unit = {

    spark.sparkContext.setLogLevel("ERROR")

    spark.sparkContext.addFile(
      "https://github.com/kotekaman/spark-structured-streaming-example/raw/ahmed_tasks/transactions.json"
    )
    val data_frame = spark.read.json(SparkFiles.get("transactions.json"))

    val transactions_table = data_frame
      .withColumn("id", col("_id.$oid"))
      .withColumn("account_id", col("account_id.$numberInt"))
      .withColumn(
        "bucket_start_date",
        col("bucket_start_date.$date.$numberLong")
      )
      .withColumn("bucket_end_date", col("bucket_end_date.$date.$numberLong"))
      .withColumn("transactions", explode(col("transactions")))
      .withColumn("amount", col("transactions.amount.$numberInt"))
      .withColumn("date", col("transactions.date.$date.$numberLong"))
      .withColumn("price", col("transactions.price"))
      .withColumn("symbol", col("transactions.symbol"))
      .withColumn("total", col("transactions.total"))
      .withColumn("transaction_code", col("transactions.transaction_code"))
      .drop(col("transactions"))
      .drop(col("_id"))
      .drop("transaction_count")

    val windowSpecbybucketDate = Window
      .partitionBy("bucket_end_date", "bucket_start_date")
      .orderBy("bucket_end_date", "bucket_start_date")

    transactions_table.printSchema()

    val windowing_df = transactions_table
      .withColumn(
        "total_between_bucket_end_date_and_bucket_start_date",
        sum("total").over(windowSpecbybucketDate)
      )
      .withColumn(
        "avg_between_bucket_end_date_and_bucket_start_date",
        avg("total").over(windowSpecbybucketDate)
      )
      .withColumn(
        "min_between_bucket_end_date_and_bucket_start_date",
        min(col("total")).over(windowSpecbybucketDate)
      )
      .withColumn(
        "max_between_bucket_end_date_and_bucket_start_date",
        max(col("total")).over(windowSpecbybucketDate)
      )
      .select(
        "bucket_end_date",
        "bucket_start_date",
        "total_between_bucket_end_date_and_bucket_start_date",
        "avg_between_bucket_end_date_and_bucket_start_date",
        "min_between_bucket_end_date_and_bucket_start_date",
        "max_between_bucket_end_date_and_bucket_start_date"
      )
      .distinct()
      .orderBy(desc("total_between_bucket_end_date_and_bucket_start_date"))

    windowing_df.write.format("delta").mode("overwrite").save(delta_lake_path)

  }
}
