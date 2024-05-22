package moni_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._

import java.util.Properties

object B_exact_2_3_4_5_6 {
  def main(args: Array[String]): Unit = {
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


    // todo (2)
    val sku_info = spark.read.jdbc(mysql_url, "sku_info", prop)
    val sku_info_max = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/sku_info")
      .select(max(col("create_time"))).first().getTimestamp(0)

    sku_info
      .filter(col("create_time") > sku_info_max)
      .withColumn("etl_date", lit("20240521"))
      .write.format("hudi").mode("append")
      .option("hoodie.table.name", "sku_info")
      .option(PARTITIONPATH_FIELD.key(), "etl_date")
      .option(PRECOMBINE_FIELD.key(), "create_time")
      .option(RECORDKEY_FIELD.key(), "id")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/sku_info")

    spark.sql("msck repair table ods_ds_hudi.sku_info")
    spark.sql("show partitions ods_ds_hudi.sku_info").show()


    // todo (3)
    val base_province = spark.read.jdbc(mysql_url, "base_province", prop)
    base_province
      .withColumn("create_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", lit("20240521"))
      .write.format("hudi").mode("append")
      .option("hoodie.table.name", "base_province")
      .option(PARTITIONPATH_FIELD.key(), "etl_date")
      .option(PRECOMBINE_FIELD.key(), "create_time")
      .option(RECORDKEY_FIELD.key(), "id")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/base_province")

    spark.sql("msck repair table ods_ds_hudi.base_province")
    spark.sql("show partitions ods_ds_hudi.base_province").show()

    // todo (4)
    val base_region = spark.read.jdbc(mysql_url, "base_region", prop)

    base_region
      .withColumn("create_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", lit("20240521"))
      .write.format("hudi").mode("append")
      .option("hoodie.table.name", "base_region")
      .option(PARTITIONPATH_FIELD.key(), "etl_date")
      .option(PRECOMBINE_FIELD.key(), "create_time")
      .option(RECORDKEY_FIELD.key(), "id")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/base_region")


    spark.sql("msck repair table ods_ds_hudi.base_region")
    spark.sql("show partitions ods_ds_hudi.base_region").show()

    // todo (5)
    val order_info = spark.read.jdbc(mysql_url, "order_info", prop)

    order_info
      .withColumn("etl_date", lit("20240521"))
      .write.format("hudi").mode("append")
      .option("hoodie.table.name", "order_info")
      .option(PARTITIONPATH_FIELD.key(), "etl_date")
      .option(PRECOMBINE_FIELD.key(), "operate_time")
      .option(RECORDKEY_FIELD.key(), "id")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/order_info")

    spark.sql("msck repair table ods_ds_hudi.order_info")
    spark.sql("show partitions ods_ds_hudi.order_info").show()


    // todo (6)
    val order_detail = spark.read.jdbc(mysql_url, "order_detail", prop)
    order_detail
      .withColumn("etl_date", lit("20240521"))
      .write.format("hudi").mode("append")
      .option("hoodie.table.name", "order_detail")
      .option(PARTITIONPATH_FIELD.key(), "etl_date")
      .option(PRECOMBINE_FIELD.key(), "create_time")
      .option(RECORDKEY_FIELD.key(), "id")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/order_detail")

    spark.sql("msck repair table ods_ds_hudi.order_detail")
    spark.sql("show partitions ods_ds_hudi.order_detail").show()


    spark.stop()


  }
}
