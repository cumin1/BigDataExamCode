package moni_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._

import java.util.Properties

object B_exact {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("数据抽取")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val prop = new Properties() {
      {
        setProperty("user", "root")
        setProperty("password", "123456")
        setProperty("driver", "com.mysql.jdbc.Driver")
      }
    }
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"

    val user_info = spark.read.jdbc(mysql_url, "user_info", prop)
    val sku_info = spark.read.jdbc(mysql_url, "sku_info", prop)
    val base_province = spark.read.jdbc(mysql_url, "base_province", prop)
    val base_region = spark.read.jdbc(mysql_url, "base_region", prop)
    val order_info = spark.read.jdbc(mysql_url, "order_info", prop)
    val order_detail = spark.read.jdbc(mysql_url, "order_detail", prop)


    val user_info_max = spark.table("ods.user_info")
      .select(max(greatest(col("operate_time"), col("create_time")))).first().getTimestamp(0)

    val ods_user_info = user_info
      .withColumn("operate_time",
        when(col("operate_time").isNull, col("create_time"))
          .otherwise(col("operate_time"))
      )
      .withColumn("max_time", greatest(col("operate_time"), col("create_time")))
      .filter(col("max_time") > user_info_max)
      .drop("max_time")
      .withColumn("etl_date", lit("20240602"))

    val sku_info_max = spark.table("ods.sku_info")
      .select(max(col("create_time"))).first().getTimestamp(0)

    val ods_sku_info = sku_info
      .filter(col("create_time") > sku_info_max)
      .withColumn("etl_date", lit("20240602"))

    val ods_base_province = base_province
      .withColumn("create_time", to_timestamp(current_timestamp()))
      .withColumn("etl_date", lit("20240602"))

    val ods_base_region = base_region
      .withColumn("create_time", to_timestamp(current_timestamp()))
      .withColumn("etl_date", lit("20240602"))

    val order_info_max = spark.table("ods.order_info")
      .select(max(greatest(col("operate_time"), col("create_time")))).first().getTimestamp(0)

    val ods_order_info = order_info
      .withColumn("operate_time",
        when(col("operate_time").isNull, col("create_time"))
          .otherwise(col("operate_time"))
      )
      .withColumn("max_time", greatest(col("operate_time"), col("create_time")))
      .filter(col("max_time") > order_info_max)
      .drop("max_time")
      .withColumn("etl_date", lit("20240602"))


    val order_detail_max = spark.table("ods.order_detail")
      .select(max(col("create_time"))).first().getTimestamp(0)

    val ods_order_detail = order_detail.filter(col("create_time") > order_detail_max)
      .withColumn("etl_date", lit("20240602"))


    ods_user_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.user_info")
    ods_sku_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.sku_info")
    ods_base_province.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.base_province")
    ods_base_region.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.base_region")
    ods_order_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.order_info")
    ods_order_detail.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.order_detail")


    spark.sql("show partitions ods.user_info").show()
    spark.sql("show partitions ods.sku_info").show()
    spark.sql("show partitions ods.base_province").show()
    spark.sql("show partitions ods.base_region").show()
    spark.sql("show partitions ods.order_info").show()
    spark.sql("show partitions ods.order_detail").show()

  }
}
