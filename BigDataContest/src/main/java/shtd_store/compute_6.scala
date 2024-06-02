package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_6 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("电商 用户占比")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val fact_order_info = spark.table("dwd.fact_order_info").filter(col("etl_date") =!= "20210101")
      .select("user_id","create_time")
      .withColumn("day",dayofmonth(col("create_time")))
      .drop("create_time")

    fact_order_info.show()

    fact_order_info
      .withColumn("lead",lead("day",1).over(Window.partitionBy("user_id").orderBy("day")))
      .filter(col("lead").isNotNull)
      .withColumn("contuine",abs(col("day") - col("day")))
      .filter(col("contuine") === 1)
      .show()



  }
}
