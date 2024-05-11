package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

object exact {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("省赛题目 抽取")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.parquet.writeLegacyFormat","true")
      .getOrCreate()

    val yesDay = date_format(date_sub(current_date(),1),"yyyyMMdd")
    val prop = new Properties() {
      {
        setProperty("driver", "com.mysql.jdbc.Driver")
        setProperty("user", "root")
        setProperty("password", "123456")
      }
    }

    val customer_inf = spark.read.jdbc("jdbc:mysql://bigdata1:3306/ds_db01?useSSL=false", "customer_inf", prop)
    customer_inf.withColumn("etl_date",lit(yesDay)).write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.customer_inf")

    val product_info = spark.read.jdbc("jdbc:mysql://bigdata1:3306/ds_db01?useSSL=false", "product_info", prop)
    product_info.withColumn("etl_date",lit(yesDay)).write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.product_info")

    val order_master = spark.read.jdbc("jdbc:mysql://bigdata1:3306/ds_db01?useSSL=false", "order_master", prop)
    order_master.withColumn("etl_date",lit(yesDay)).write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.order_master")

    val order_detail = spark.read.jdbc("jdbc:mysql://bigdata1:3306/ds_db01?useSSL=false", "order_detail", prop)
    order_detail.withColumn("etl_date",lit(yesDay)).write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.order_detail")


  }
}
