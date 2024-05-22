package moni_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    val user_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/user_info").filter(col("etl_date") === "20240521")
    val sku_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/sku_info").filter(col("etl_date") === "20240521")
//    val base_province = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/base_province")
//    val base_region = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/base_region")
//    val order_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/order_info")
//    val order_detail = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/order_detail")

    user_info.show()
    sku_info.show()
//    base_province.show()
//    base_region.show()
//    order_info.show()
//    order_detail.show()

    user_info.write.format("parquet").mode("overwrite").save("file:///D:\\data\\ods_data\\user_info")
    sku_info.write.format("parquet").mode("overwrite").save("file:///D:\\data\\ods_data\\sku_info")
//    base_province.write.format("parquet").mode("overwrite").save("file:///D:\\data\\ods_data\\base_province")
//    base_region.write.format("parquet").mode("overwrite").save("file:///D:\\data\\ods_data\\base_region")
//    order_info.write.format("parquet").mode("overwrite").save("file:///D:\\data\\ods_data\\order_info")
//    order_detail.write.format("parquet").mode("overwrite").save("file:///D:\\data\\ods_data\\order_detail")

    spark.stop()
  }
}
