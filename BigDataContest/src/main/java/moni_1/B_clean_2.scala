package moni_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.expressions.Window

import java.util.Properties

object B_clean_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("数据清洗")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val ods_sku_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/sku_info")
      .filter(col("etl_date") === "20240521")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

    val dim_sku_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")
      .drop("etl_date")

    val result2 = ods_sku_info.union(dim_sku_info)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("create_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("etl_date", lit("20240521"))


    result2
      .write.format("hudi").mode("append")
      .option("hoodie.table.name", "dim_sku_info")
      .option(PARTITIONPATH_FIELD.key(), "etl_date")
      .option(PRECOMBINE_FIELD.key(), "dwd_modify_time")
      .option(RECORDKEY_FIELD.key(), "id")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")

    spark.sql("msck repair table dwd_ds_hudi.dim_sku_info")
    spark.sql("show partitions dwd_ds_hudi.dim_sku_info").show()
    spark.sql("select id,sku_desc,dwd_insert_user,dwd_modify_time,etl_date from dwd_ds_hudi.dim_sku_info where etl_date = 20240521 and id >= 15 and id <= 20 order by id asc").show()


  }
}
