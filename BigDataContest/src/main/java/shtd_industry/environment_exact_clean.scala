package shtd_industry

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._

import java.util.Properties

object environment_exact_clean {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris","thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .getOrCreate()
    val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")
    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","123456")
    val url = "jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false"

    val EnvironmentData = spark.read.jdbc(url, "EnvironmentData", prop)

    EnvironmentData.withColumn("etldate",lit("20240426"))
      .write.format("hive").mode("append")
      .partitionBy("etldate").saveAsTable("ods.environmentdata")

    spark.table("ods.environmentdata").drop("etldate")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate",lit("20240426"))
      .write.format("hive").mode("append")
      .partitionBy("etldate").saveAsTable("dwd.fact_environment_data")


  }
}
