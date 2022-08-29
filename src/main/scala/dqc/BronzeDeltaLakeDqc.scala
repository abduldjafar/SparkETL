package dqc
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.sql.SparkSession
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.http.impl.client.DefaultHttpClient
import com.typesafe.config.{Config => TConfig}
import javax.net.ssl.HttpsURLConnection;
import notification.Notification

object BronzeDeltaLakeDqc {

  def chekVerificationResult(
      config: TConfig,
      verificationResult: VerificationResult
  ): Unit = {

    val notification = Notification

    if (verificationResult.status == CheckStatus.Success) {
      val message = "The data passed the test, everything is fine!"
      val title = "success from dqyc"

      println(message)

      notification.toDiscord(
        title,
        config,
        message
      )
    } else {
      println("We found errors in the data:\n")

      var message = ""
      val title = "Data Quality Check Has Some Failures"

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          val temp_message = s"\\n${result.constraint}: ${result.message.get}"
          
          message = message + temp_message
          message = message + "\\n=============================================================="
        }

      notification.toDiscord(
        title,
        config,
        message
      )

      println("exiting......")
      System.exit(1)
    }
  }

  def dqcTbMainAirbnbDatasetInBronzeDeltaLake(
      spark: SparkSession,
      config: TConfig,
      delta_lake_path: String
  ): Unit = {
    val data =
      spark.read.format("delta").load(delta_lake_path.concat("/tb_main"))
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "airbnb datasets")
          .isComplete("id")
          .hasDataType("accommodates", ConstrainableDataTypes.Numeric)
          .hasDataType("availability_30", ConstrainableDataTypes.Numeric)
          .hasDataType("availability_365", ConstrainableDataTypes.Numeric)
          .hasDataType("availability_60", ConstrainableDataTypes.Numeric)
          .hasDataType("availability_90", ConstrainableDataTypes.Numeric)
          .hasDataType("bathrooms", ConstrainableDataTypes.Numeric)
          .isPositive("bathrooms")
          .hasDataType("bedrooms", ConstrainableDataTypes.Numeric)
          .isPositive("bedrooms")
          .hasDataType("beds", ConstrainableDataTypes.Numeric)
          .isPositive("beds")
          .hasDataType("calendar_last_scraped",ConstrainableDataTypes.Numeric)
          .isPositive("calendar_last_scraped")
          .hasDataType("cleaning_fee",ConstrainableDataTypes.Numeric)
          .isPositive("cleaning_fee")
          .hasDataType("first_review",ConstrainableDataTypes.Numeric)
          .isPositive("first_review")
          .hasDataType("guests_included",ConstrainableDataTypes.Numeric)
          .isPositive("guests_included")
          .hasDataType("host_listings_count",ConstrainableDataTypes.Numeric)
          .isPositive("host_listings_count")
          .hasDataType("host_response_rate", ConstrainableDataTypes.Numeric)
          .isPositive("host_response_rate")
          .hasDataType("last_review", ConstrainableDataTypes.Numeric)
          .isPositive("last_review")
          .hasDataType("monthly_price", ConstrainableDataTypes.Numeric)
          isPositive("monthly_price")
      )
      .run()
    chekVerificationResult(config, verificationResult)

  }
}
