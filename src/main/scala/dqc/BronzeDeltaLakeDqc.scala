package dqc
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.sql.SparkSession

object BronzeDeltaLakeDqc {
  def dqcInBronzeDeltaLake(
      spark: SparkSession,
      delta_lake_path: String
  ): Unit = {
    val data = spark.read.format("delta").load(delta_lake_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "unit testing my data")
          .hasDataType("accommodates", ConstrainableDataTypes.Numeric)
          .hasSize(_ >= 100)
      )
      .run()
  }
}
