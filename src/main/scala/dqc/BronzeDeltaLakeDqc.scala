package dqc
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.sql.SparkSession
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import java.io.OutputStream;
import java.net.URL;
import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import com.google.gson.Gson

import javax.net.ssl.HttpsURLConnection;
import java.net.{http, URI}

object BronzeDeltaLakeDqc {

  def notificationTestResultDIscord(
      title: String,
      message: String
  ): Unit = {

    val request = http.HttpRequest
      .newBuilder()
      .uri(
        URI.create(
          ""
        )
      )
      .header("Content-Type", "application/json")
      .method(
        "POST",
        http.HttpRequest.BodyPublishers.ofString(
          "{\n    \"embeds\": [\n    {\n      \"title\": \" " + title + " \",\n      \"description\": \"" + message + "\",\n      \"color\": 15258703\n}]}"
        )
      )
      .build();

    val response = http.HttpClient
      .newHttpClient()
      .send(request, http.HttpResponse.BodyHandlers.ofString());
    println(response.statusCode())

  }

  def chekVerificationResult(verificationResult: VerificationResult): Unit = {
    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
      notificationTestResultDIscord(
            "success from dqyc",
            "The data passed the test, everything is fine!"
          )
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          println(s"${result.constraint}: ${result.message.get}")
          notificationTestResultDIscord(
            "error from dqyc",
            s"${result.constraint}: ${result.message.get}"
          )
        }

      println("exiting......")
      System.exit(1)
    }
  }

  def checkCountFailure(
      spark: SparkSession,
      verificationResult: VerificationResult
  ): Unit = {
    val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
    val failure_count = resultDataFrame
      .select("constraint_status")
      .where("constraint_status='Failure'")
      .count()
    if (failure_count > 0) {
      println("Threre are failures in Data Quality Check")
      resultDataFrame.show()
      println("exiting......")
      System.exit(1)
    }
  }

  def dqcTbMainAirbnbDatasetInBronzeDeltaLake(
      spark: SparkSession,
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
    chekVerificationResult(verificationResult)

  }
}
