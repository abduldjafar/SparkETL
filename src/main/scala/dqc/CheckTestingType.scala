package dqc

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.sql.SparkSession
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

object CheckTestingType {

  def dqcShowTestingType(
      spark: SparkSession,
      delta_lake_path: String
  ): Unit = {
    import spark.implicits._
    val data = spark.read.format("delta").load(delta_lake_path)

    val suggestionResult = {
      ConstraintSuggestionRunner()
        .onData(data)
        .addConstraintRules(Rules.DEFAULT)
        .run()
    }

    val suggestionDataFrame = suggestionResult.constraintSuggestions
      .flatMap { case (column, suggestions) =>
        suggestions.map { constraint =>
          (column, constraint.description, constraint.codeForConstraint)
        }
      }
      .toSeq
      .toDS()
    suggestionDataFrame.show(false)

  }

}
