package etl.silverDeltaLake
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  row_number,
  monotonically_increasing_id,
  asc,
  lit
}

object RdbmsDataWarehousing {
  def process_db_employees_from_bronze(
      spark: SparkSession,
      sources_delta_lake_path: String,
      destination_delta_lake_path: String
  ): Unit = {

    def read_datas_from_delta_lake(delta_lake_path: String): DataFrame = {

      return spark.read.format("delta").load(delta_lake_path)

    }

    val tb_current_dept_emp = read_datas_from_delta_lake(
      sources_delta_lake_path.concat(
        "current_dept_emp"
      )
    )
    val tb_departments = read_datas_from_delta_lake(
      sources_delta_lake_path.concat("departments")
    )
    val tb_dept_emp = read_datas_from_delta_lake(
      sources_delta_lake_path.concat("dept_emp")
    )
    val tb_dept_emp_latest_date = read_datas_from_delta_lake(
      sources_delta_lake_path.concat(
        "dept_emp_latest"
      )
    )
    val tb_dept_manager = read_datas_from_delta_lake(
      sources_delta_lake_path.concat("dept_manager")
    )
    val tb_employees = read_datas_from_delta_lake(
      sources_delta_lake_path.concat("employees")
    )
    val tb_salaries = read_datas_from_delta_lake(
      sources_delta_lake_path.concat("salaries")
    )
    val tb_titles = read_datas_from_delta_lake(
      sources_delta_lake_path.concat("titles")
    )

    val title_with_id = tb_titles
      .select("title")
      .distinct()
      .orderBy(asc("title"))
      .withColumn("titles_id", monotonically_increasing_id() + lit(1))
      .withColumnRenamed("title", "temp_title")

    val dim_title_with_emp_no = tb_titles
      .join(
        title_with_id,
        tb_titles("title") === title_with_id("temp_title"),
        "inner"
      )
      .select("titles_id", "title", "emp_no", "from_date", "to_date")
      .withColumn(
        "titles_surrogate_key",
        monotonically_increasing_id() + lit(1)
      )
      .withColumnRenamed("emp_no", "titles_emp_no")

    val fact_employees = tb_employees
      .join(
        dim_title_with_emp_no,
        tb_employees("emp_no") === dim_title_with_emp_no("titles_emp_no"),
        "inner"
      )
      .select("emp_no", "titles_surrogate_key")
      .withColumnRenamed("titles_surrogate_key", "titles_key")
      .withColumn("key", monotonically_increasing_id() + lit(1))

    fact_employees.write
      .format("delta")
      .mode("overwrite")
      .save(destination_delta_lake_path.concat("fact_employees"))
  }

}
