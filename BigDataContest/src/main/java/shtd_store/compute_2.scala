package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object compute_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算2")
      .master("local[*]").enableHiveSupport()
      .config("hive.exec.partition.mode", "nonstrict")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .getOrCreate()

    val fact_order_info = spark.table("dwd.fact_order_info")

    val result = fact_order_info.select(
        "city", "province", "order_money", "create_time"
      )
      .withColumn("year", year(from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("month", month(from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"), "yyyy-MM-dd HH:mm:ss")))
      .groupBy("city", "province", "year", "month")
      .agg(
        sum("order_money") as "total_amount",
        count("order_money") as "total_count"
      )
      .withColumn("sequence", row_number().over(Window.partitionBy("province", "year", "month").orderBy(desc("total_amount"))))
      .withColumnRenamed("province", "province_name")
      .withColumnRenamed("city", "city_name")
      .withColumn("total_amount", col("total_amount").cast(DataTypes.DoubleType))
      .select("city_name", "province_name", "total_amount", "total_count", "sequence", "year", "month")


    result.show()
    result.write.format("hive").mode("append").saveAsTable("dws.city_consumption_day_aggr")


  }
}
