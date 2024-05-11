package Utils

import org.apache.spark.sql.SparkSession

object Repair_table {
  def repair_table(spark: SparkSession, database: String, table: String, savepath: String): Unit = {
    spark.sql(s"create database if not exists ${database}")
    spark.sql(s"create table if not exists ${database}.${table} using hudi location '${savepath}'")
    spark.sql(s"msck repair table ${database}.${table}")
    spark.sql(s"refresh table ${database}.${table}")
  }

  def show_partition(spark: SparkSession, database: String, table: String): Unit = {
    spark.sql(s"show partitions ${database}.${table}").show()
  }
}
