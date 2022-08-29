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
      val title = "error from dqyc"

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          message.concat(s"\\n${result.constraint}: ${result.message.get}")
        }

      notification.toDiscord(
        title,
        config,
        message
      )

      println(message)
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
      )
      .run()
    chekVerificationResult(config, verificationResult)

  }
}
