package etl.bronzeDeltaLake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkFiles
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{IntegerType, DoubleType, BooleanType}
import org.apache.spark.sql.functions.{
  explode,
  col,
  avg,
  sum,
  desc,
  min,
  max,
  slice
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
      "address_location_cordinates_long" -> 
        slice(
          col("address.location.coordinates"),
          1,
          1
        ),
      "address_location_cordinates_lat" -> 
        slice(
          col("address.location.coordinates"),
          2,
          1
        ),
      "address_location_is_exact" -> col("address.location.is_location_exact"),
      "address_location_type" -> col("address.location.type"),
      "address_market" -> col("address.market"),
      "address_street" -> col("address.street"),
      "address_suburb" -> col("address.suburb"),
      "amenities" -> col("amenities"),
      "availability_30" -> col("availability.availability_30.$numberInt"),
      "availability_365" -> col("availability.availability_365.$numberInt"),
      "availability_60" -> col("availability.availability_60.$numberInt"),
      "availability_90" -> col("availability.availability_90.$numberInt")
    )

    val cleanned_datas = data_frame
      .withColumns(colsMap)
      .withColumn(
        "address_location_cordinates_long",
        explode(col("address_location_cordinates_long"))
      )
      .withColumn(
        "address_location_cordinates_long",
        col("address_location_cordinates_long.$numberDouble")
      )
      .withColumn(
        "address_location_cordinates_lat",
        explode(col("address_location_cordinates_lat"))
      )
      .withColumn(
        "address_location_cordinates_lat",
        col("address_location_cordinates_lat.$numberDouble")
      )
      .withColumn(
        "amenities",
        explode(col("amenities"))
      )
      .select(
        "id",
        "accommodates",
        "address_country",
        "address_government_area",
        "address_location_cordinates_long",
        "address_location_cordinates_lat",
        "address_location_is_exact",
        "address_location_type",
        "amenities",
        "availability_30",
        "availability_365",
        "availability_60",
        "availability_90"
      )

    cleanned_datas.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(delta_lake_path)
    cleanned_datas
      .select(
        "address_location_cordinates_long",
        "address_location_cordinates_lat"
      )
      .show(false)

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
