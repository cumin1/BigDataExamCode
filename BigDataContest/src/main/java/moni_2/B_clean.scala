package moni_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.expressions.Window

import java.util.Properties

object B_clean {
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

    val ods_user_info = spark.table("ods.user_info").filter(col("etl_date") === "20240602")
      .drop("etl_date")
      .withColumn("birthday",to_timestamp(col("birthday"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",to_timestamp(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",to_timestamp(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))

    val dwd_user_info = spark.table("dwd.dim_user_info").drop("etl_date")

    val dim_user_info = ods_user_info.union(dwd_user_info)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("operate_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("operate_time",
        when(col("operate_time").isNull, col("create_time"))
          .otherwise(col("operate_time"))
      )
      .withColumn("etl_date", lit("20240602"))

    val ods_sku_info = spark.table("ods.sku_info")
      .filter(col("etl_date") === "20240602")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

    val dwd_sku_info = spark.table("dwd.dim_sku_info").drop("etl_date")

    val dim_sku_info = ods_sku_info.union(dwd_sku_info)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("create_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("etl_date", lit("20240602"))

    val dim_province = spark.table("ods.base_province")
      .filter(col("etl_date") === "20240602")
      .drop("etl_date")
      .withColumn("create_time", to_timestamp(col("create_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", lit("20240602"))

    val dim_region = spark.table("ods.base_region")
      .filter(col("etl_date") === "20240602")
      .drop("etl_date")
      .withColumn("create_time", to_timestamp(col("create_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", lit("20240602"))


    val fact_order_info = spark.table("ods.order_info")
      .filter(col("etl_date") === "20240602")
      .drop("etl_date")
      .withColumn("operate_time",
        when(col("operate_time").isNull, col("create_time"))
          .otherwise(col("operate_time"))
      )
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", date_format(col("create_time"), "yyyyMMdd"))

    val fact_order_detail = spark.table("ods.order_detail")
      .filter(col("etl_date") === "20240602")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", date_format(col("create_time"), "yyyyMMdd"))


//    dim_user_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.dim_user_info")
//    dim_sku_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.dim_sku_info")
//    dim_province.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.dim_province")
//    dim_region.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.dim_region")
    fact_order_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.fact_order_info")
    fact_order_detail.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.fact_order_detail")

//    spark.sql("show partitions dwd.dim_user_info").show()
//    spark.sql(
//      """select id,sku_desc,dwd_insert_user,dwd_modify_time,etl_date
//        |from dwd.dim_sku_info
//        |where etl_date=20240602 and id >= 15 and id <= 20
//        |order by id asc
//        |""".stripMargin).show()
//
//    spark.sql("select count(*) from dwd.dim_province").show()
//    spark.sql("select count(*) from dwd.dim_region").show()
    spark.sql("show partitions dwd.fact_order_info").show()
    spark.sql("show partitions dwd.fact_order_detail").show()

  }
}
