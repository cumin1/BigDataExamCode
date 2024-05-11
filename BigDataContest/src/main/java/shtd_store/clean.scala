package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object clean {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("省赛题目 清洗")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")
    val yesDay = date_format(date_sub(current_date(),1),"yyyyMMdd")

    spark.table("ods.customer_inf")
      .drop("etl_date")
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(nowTime))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(nowTime))
      .withColumn("etl_date",lit(yesDay))
      .write.format("hive").mode("overwrite").partitionBy("etl_date").saveAsTable("dwd.dim_customer_inf")

    spark.table("ods.product_info")
      .drop("etl_date")
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(nowTime))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(nowTime))
      .withColumn("etl_date",lit(yesDay))
      .write.format("hive").mode("overwrite").partitionBy("etl_date").saveAsTable("dwd.dim_product_info")

    spark.table("ods.order_master")
      .drop("etl_date")
      .withColumn("create_time",from_unixtime(unix_timestamp(col("create_time"),"yyyyMMddHHmmss"),"yyyyMMdd"))
      .withColumn("shipping_time",from_unixtime(unix_timestamp(col("shipping_time"),"yyyyMMddHHmmss"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("pay_time",from_unixtime(unix_timestamp(col("pay_time"),"yyyyMMddHHmmss"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("receive_time",from_unixtime(unix_timestamp(col("receive_time"),"yyyyMMddHHmmss"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(nowTime))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(nowTime))
      .withColumn("etl_date",lit(yesDay))
      .write.format("hive").mode("overwrite").partitionBy("etl_date").saveAsTable("dwd.fact_order_info")

    spark.table("ods.order_detail")
      .drop("etl_date")
      .withColumn("create_time",from_unixtime(unix_timestamp(col("create_time"),"yyyyMMddHHmmss"),"yyyyMMdd"))
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(nowTime))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(nowTime))
      .withColumn("etl_date",lit(yesDay))
      .write.format("hive").mode("overwrite").partitionBy("etl_date").saveAsTable("dwd.fact_order_detail")

  }
}
