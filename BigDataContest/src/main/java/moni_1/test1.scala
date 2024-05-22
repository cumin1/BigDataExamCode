package moni_1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

object test1 {
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

    val dim_user_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
    val dim_sku_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")
    val dim_province = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_province")
    val dim_region = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_region")
    val fact_order_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
    val fact_order_detail = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")

    dim_user_info.show()
    dim_sku_info.show()
    dim_province.show()
    dim_region.show()
    fact_order_info.show()
    fact_order_detail.show()


  }
}
