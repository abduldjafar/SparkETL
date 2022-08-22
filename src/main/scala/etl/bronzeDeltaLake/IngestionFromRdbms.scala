package etl.bronzeDeltaLake
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col

object IngestionFromRdbms {
  def proces_employees_db(
      spark: SparkSession,
      delta_lake_path: String,
      jdbcUrl: String,
      connectionProperties: Properties
  ): Unit = {
    
    /*
    tables name in database
    +----------------------+
    | Tables_in_employees  |
    +----------------------+
    | current_dept_emp     |
    | departments          |
    | dept_emp             |
    | dept_emp_latest_date |
    | dept_manager         |
    | employees            |
    | salaries             |
    | titles               |
    +----------------------+
     */

    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    val mapping_source_in_delta_lake = Map(
      "current_dept_emp" -> delta_lake_path.concat(
        "/db_employees/current_dept_emp"
      ),
      "departments" -> delta_lake_path.concat("/db_employees/departments"),
      "dept_emp" -> delta_lake_path.concat("/db_employees/dept_emp"),
      "dept_emp_latest_date" -> delta_lake_path.concat(
        "/db_employees/dept_emp_latest"
      ),
      "dept_manager" -> delta_lake_path.concat("/db_employees/dept_manager"),
      "employees" -> delta_lake_path.concat("/db_employees/employees"),
      "salaries" -> delta_lake_path.concat("/db_employees/salaries"),
      "titles" -> delta_lake_path.concat("/db_employees/titles")
    )

    mapping_source_in_delta_lake.foreach((data) =>
      spark.read
        .jdbc(jdbcUrl, data._1, connectionProperties)
        .write
        .format("delta")
        .mode("overwrite")
        .save(data._2)
    )

  }
  def proces_sakila_db(spark: SparkSession, delta_lake_path: String): Unit = {}
}
