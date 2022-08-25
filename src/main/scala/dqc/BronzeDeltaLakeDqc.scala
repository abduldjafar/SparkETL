package dqc
import com.amazon.deequ.{VerificationSuite,VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.sql.SparkSession
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

object BronzeDeltaLakeDqc {

  def checkCountFailure(spark : SparkSession,verificationResult: VerificationResult): Unit = {
    val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
    val failure_count = resultDataFrame.select("constraint_status").where("constraint_status='Failure'").count()
    if (failure_count > 0) {
        println("Threre are failures in Data Quality Check")
        resultDataFrame.show()
        println("exiting......")
        System.exit(1)
    }
  }

  def dqcAirbnbDatasetInBronzeDeltaLake(
      spark: SparkSession,
      delta_lake_path: String
  ): Unit = {
    val data = spark.read.format("delta").load(delta_lake_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "airbnb datasets")
          .hasDataType("accommodates", ConstrainableDataTypes.Numeric)
          .isComplete("address_location_cordinates_long")
          .isNonNegative("address_location_cordinates_lat")
      )
      .run()
    checkCountFailure(spark, verificationResult)

  }
}
