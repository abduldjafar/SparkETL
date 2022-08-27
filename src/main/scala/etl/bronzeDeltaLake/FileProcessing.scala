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
  slice,
  trim,
  ltrim,
  rtrim
}

object FileProcessing {
  def process_airbnb(
      spark: SparkSession,
      data_source: String,
      delta_lake_path: String
  ): Unit = {

    var data_frame = spark.read.json(
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
      "availability_90" -> col("availability.availability_90.$numberInt"),
      "bathrooms" -> col("bathrooms.$numberDecimal"),
      "bedrooms" -> col("bedrooms.$numberInt"),
      "beds" -> col("beds.$numberInt"),
      "calendar_last_scraped" -> col("calendar_last_scraped.$date.$numberLong"),
      "cleaning_fee" -> col("cleaning_fee.$numberDecimal"),
      "extra_people" -> col("extra_people.$numberDecimal"),
      "first_review" -> col("first_review.$date.$numberLong"),
      "guests_included" -> col("guests_included.$numberDecimal"),
      "host_about" -> col("host.host_about"),
      "host_has_profile_pic" -> col("host.host_has_profile_pic"),
      "host_id" -> col("host.host_id"),
      "host_identity_verified" -> col("host.host_identity_verified"),
      "host_is_superhost" -> col("host.host_is_superhost"),
      "host_listings_count" -> col("host.host_listings_count.$numberInt"),
      "host_location" -> col("host.host_location"),
      "host_name" -> col("host.host_name"),
      "host_neighbourhood" -> col("host.host_neighbourhood"),
      "host_picture_url" -> col("host.host_picture_url"),
      "host_response_rate" -> col("host.host_response_rate.$numberInt"),
      "host_response_time" -> col("host.host_response_time"),
      "host_thumbnail_url" -> col("host.host_thumbnail_url"),
      "host_total_listings_count" -> col(
        "host.host_total_listings_count.$numberInt"
      ),
      "host_url" -> col("host.host_url"),
      "host_verifications" -> col("host.host_verifications"),
      "images_medium_url" -> col("images.medium_url"),
      "images_picture_url" -> col("images.picture_url"),
      "images_thumbnail_url" -> col("images.thumbnail_url"),
      "images_xl_picture_url" -> col("images.xl_picture_url"),
      "last_review" -> col("last_review.$date.$numberLong"),
      "last_scraped" -> col("last_scraped.$date.$numberLong"),
      "monthly_price" -> col("monthly_price.$numberDecimal"),
      "price" -> col("price.$numberDecimal"),
      "review_scores_accuracy" -> col(
        "review_scores.review_scores_accuracy.$numberInt"
      ),
      "review_scores_checkin" -> col(
        "review_scores.review_scores_checkin.$numberInt"
      ),
      "review_scores_cleanliness" -> col(
        "review_scores.review_scores_cleanliness.$numberInt"
      ),
      "review_scores_communication" -> col(
        "review_scores.review_scores_communication.$numberInt"
      ),
      "review_scores_location" -> col(
        "review_scores.review_scores_location.$numberInt"
      ),
      "review_scores_rating" -> col(
        "review_scores.review_scores_rating.$numberInt"
      ),
      "review_scores_value" -> col(
        "review_scores.review_scores_value.$numberInt"
      ),
      "reviews_per_month" -> col("reviews_per_month.$numberInt"),
      "security_deposit" -> col("security_deposit.$numberDecimal"),
      "weekly_price" -> col("weekly_price.$numberDecimal")
    )

    colsMap.foreach { data =>
      {
        data_frame = data_frame.withColumn(data._1, data._2)
      }
    }

    data_frame.dtypes.foreach(f => {
      if (f._2 == "StringType") {
        data_frame = data_frame.withColumn(f._1, trim(col(f._1)))
      }
    })
    val reviewed_df = data_frame
      .withColumn(
        "reviews",
        explode(col("reviews"))
      )
      .withColumn(
        "reviews_id",
        col("reviews._id")
      )
      .withColumn(
        "reviews_comments",
        col("reviews.comments")
      )
      .withColumn(
        "reviews_date",
        col("reviews.date.$date.$numberLong")
      )
      .withColumn(
        "reviews_listing_id",
        col("reviews.listing_id")
      )
      .withColumn(
        "reviewer_id",
        col("reviews.reviewer_id")
      )
      .withColumn(
        "reviewer_name",
        col("reviews.reviewer_name")
      )
      .select(
        "id","reviews_id", "reviews_comments", "reviews_date", "reviews_listing_id","reviewer_id","reviewer_name"
      )

    val cleanned_datas_temp = data_frame
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
      .withColumn(
        "host_verifications",
        explode(col("host_verifications"))
      )
      

    val cleanned_datas = cleanned_datas_temp.select(
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
      "availability_90",
      "bathrooms",
      "bed_type",
      "cancellation_policy",
      "description",
      "extra_people",
      "first_review",
      "guests_included",
      "host_about",
      "host_has_profile_pic",
      "host_id",
      "host_identity_verified",
      "host_is_superhost",
      "host_listings_count",
      "host_location",
      "host_name",
      "host_neighbourhood",
      "host_picture_url",
      "host_response_rate",
      "host_response_time",
      "host_thumbnail_url",
      "host_total_listings_count",
      "host_url",
      "host_verifications",
      "house_rules",
      "interaction",
      "last_review",
      "last_scraped",
      "listing_url",
      "maximum_nights",
      "minimum_nights",
      "name",
      "neighborhood_overview",
      "notes",
      "price",
      "property_type",
      "review_scores_accuracy",
      "review_scores_checkin",
      "review_scores_cleanliness",
      "review_scores_communication",
      "review_scores_location",
      "review_scores_rating",
      "review_scores_value",
      "reviews_per_month",
      "room_type",
      "security_deposit",
      "space",
      "summary",
      "transit",
      "weekly_price"
    )

    cleanned_datas.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(delta_lake_path.concat("/tb_main"))
    
    reviewed_df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(delta_lake_path.concat("/tb_reviews"))

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
