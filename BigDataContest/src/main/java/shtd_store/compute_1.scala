package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object compute_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("省赛题目 计算")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val customer_inf = spark.table("dwd.dim_customer_inf").select("customer_id", "customer_name").withColumnRenamed("customer_id","c_id")

    val order_info = spark.table("dwd.fact_order_info").select("customer_id", "order_money", "create_time")

    val result = order_info.join(customer_inf, order_info("customer_id") === customer_inf("c_id"))
      .withColumn("create_time", from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("year", year(col("create_time")))
      .withColumn("month", month(col("create_time")))
      .withColumn("day", dayofmonth(col("create_time")))
      .groupBy("customer_id", "customer_name", "year", "month", "day")
      .agg(
        sum("order_money") as "total_amount",
        count("order_money") as "total_count"
      )
      .select("customer_id", "customer_name", "total_amount", "total_count", "year", "month", "day")

    result.show()

    result.write.mode("overwrite").format("hive").saveAsTable("dws.user_consumption_day_aggr")


  }
}
