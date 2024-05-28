package moni_1

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit, to_timestamp, when}

object B_clean_5_6 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("moni clean")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

    // todo 5
    val order_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/order_info")
      .filter(col("etl_date") === "20240521")
      .drop("etl_date")


    order_info
      .withColumn("operate_time",
        when(col("operate_time").isNull, col("create_time"))
          .otherwise(col("operate_time"))
      )
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", date_format(col("create_time"),"yyyyMMdd"))
      .write.format("hudi").mode("append")
      .option("hoodie.table.name", "fact_order_info")
      .option(PARTITIONPATH_FIELD.key(), "etl_date")
      .option(PRECOMBINE_FIELD.key(), "operate_time")
      .option(RECORDKEY_FIELD.key(), "id")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")

    spark.sql("create table dwd_ds_hudi.fact_order_info using hudi location 'hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info'")
    spark.sql("msck repair table dwd_ds_hudi.fact_order_info")
    spark.sql("show partitions dwd_ds_hudi.fact_order_info").show()

    // todo (6)

    spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/order_detail")
      .filter(col("etl_date") === "20240521")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", date_format(col("create_time"),"yyyyMMdd"))
      .write.format("hudi").mode("append")
      .option("hoodie.table.name", "fact_order_detail")
      .option(PARTITIONPATH_FIELD.key(), "etl_date")
      .option(PRECOMBINE_FIELD.key(), "dwd_modify_time")
      .option(RECORDKEY_FIELD.key(), "id")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")


    spark.sql("create table dwd_ds_hudi.fact_order_detail using hudi location 'hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail'")
    spark.sql("msck repair table dwd_ds_hudi.fact_order_detail")
    spark.sql("show partitions dwd_ds_hudi.fact_order_detail").show()



    spark.stop()
  }
}
