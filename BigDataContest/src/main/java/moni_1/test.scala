package moni_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import java.util.Properties

object test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("抽取")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()


    val properties = new Properties() {
      {
        setProperty("driver", "com.mysql.jdbc.Driver")
        setProperty("user", "root")
        setProperty("password", "123456")
      }
    }
    val mysql_url = "jdbc:mysql://localhost:3306/test1?useSSL=false"

//    val order_info = spark.read.jdbc(mysql_url, "order_info", properties)
//
//    val max_time_order_info = spark.read.table("ods.order_info")
//      .select(max(greatest(col("operate_time"), col("create_time"))))
//      .first().getTimestamp(0)
//
//    val ods_order_info = order_info
//      .withColumn("max_time", greatest(col("operate_time"), col("create_time")))
//      .filter(col("max_time") > max_time_order_info)
//      .drop("max_time")
//      .withColumn("etl_date", lit("20240530"))
//
//    ods_order_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.order_info")
val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")
    val order_info = spark.table("ods.order_info").filter(col("etl_date") === "20240530")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val res5 = order_info
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("create_time")))
      .withColumn("etl_date", date_format(col("create_time"), "yyyyMMdd"))

    res5.write.format("hive").mode("overwrite").partitionBy("etl_date").saveAsTable("dwd.fact_order_info")

    spark.stop()
  }
}
