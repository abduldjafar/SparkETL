package etl.bronzeDeltaLake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, col, avg, sum, desc, min, max}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkFiles
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{
      IntegerType,
      DoubleType,
      BooleanType
}

object FileProcessing {
  def process_airbnb(
      spark: SparkSession,
      data_source: String,
      delta_lake_path: String
  ): Unit = {

    val data_frame = spark.read.json(
      data_source.concat("sample_airbnb/listingsAndReviews.json")
    )

    val colsMap: Map[String, Column] = Map(
      "id" -> col("_id"),
      "accommodates" -> col("accommodates.$numberInt").cast(IntegerType),
      "address_country" -> col("address.country"),
      "address_country_code" -> col("address.country_code"),
      "address_government_area" -> col("address.government_area"),
      "address_location_cordinates_element_float" ->col("address.location.cordinates.element.$numberDouble").cast(DoubleType),
      "address_location_cordinates_element_int" -> col("address.location.cordinates.element.$numberInt").cast(IntegerType),
      "address_location_is_exact" -> col("address.location.is_location_exact").cast(BooleanType),
      "address_location_type" -> col("address.location.type")
    )

    data_frame
      .withColumns(colsMap)
      .select(
        "id",
        "accommodates",
        "address_country",
        "address_government_area",
        "address_location_cordinates_element_float",
        "address_location_cordinates_element_int",
        "address_location_is_exact",
        "address_location_type"
      )
      .write.format("delta").mode("overwrite").save(delta_lake_path)

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
