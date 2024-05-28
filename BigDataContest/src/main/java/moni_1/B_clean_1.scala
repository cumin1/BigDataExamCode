package moni_1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.expressions.Window

import java.util.Properties
object B_clean_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("数据清洗")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()


    val ods_user_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/user_info")
      .filter(col("etl_date") === "20240521")
      .drop("etl_date")
      .withColumn("birthday",to_timestamp(col("birthday"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",to_timestamp(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",to_timestamp(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
    val dim_user_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
      .drop("etl_date")

    val result1 = ods_user_info.union(dim_user_info)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("operate_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("etl_date", lit("20240521"))

    result1
      .write.format("hudi").mode("append")
      .option("hoodie.table.name","dim_user_info")
      .option(PARTITIONPATH_FIELD.key(),"etl_date")
      .option(PRECOMBINE_FIELD.key(),"operate_time")
      .option(RECORDKEY_FIELD.key(),"id")
      .option("hoodie.datasource.write.hive_style_partitioning","true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")

//    spark.sql("create table dwd_ds_hudi.dim_user_info using hudi location 'hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info'")
    spark.sql("msck repair table dwd_ds_hudi.dim_user_info")
    spark.sql("show partitions dwd_ds_hudi.dim_user_info").show()

    spark.stop()
  }
}
