package etl.silverDeltaLake
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrame

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

    val dim_tb_employees = tb_employees
  }

}
