package Utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit, when}

import java.util.Properties

object Create_ds_hive_test_data {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("导入测试用数据")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","123456")
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"

    // todo hive的模拟数据 采用从mysql抽取一部分的方法模拟
    // todo ods层数据
    val user_info = spark.read.jdbc(mysql_url, "user_info", prop)
      .withColumn("operate_time",when(col("operate_time").isNull,col("create_time")).otherwise(col("operate_time")))
      .filter(col("operate_time") === "2020-04-26 00:12:14")
      .withColumn("etl_date",lit("20210101"))

    val sku_info = spark.read.jdbc(mysql_url, "sku_info", prop)
      .filter(col("create_time") === "2021-01-01 12:21:13")
      .withColumn("etl_date", lit("20210101"))

    val base_province = spark.read.jdbc(mysql_url, "base_province", prop)
      .filter(col("id") <= 12)
      .withColumn("create_time",date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", lit("20210101"))

    val base_region = spark.read.jdbc(mysql_url, "base_region", prop)
      .filter(col("id") <= 2)
      .withColumn("create_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", lit("20210101"))

    val order_info = spark.read.jdbc(mysql_url, "order_info", prop)
      .filter(col("operate_time") === "2020-04-25 18:47:14")
      .withColumn("etl_date", lit("20210101"))

    val order_detail = spark.read.jdbc(mysql_url, "order_detail", prop)
      .filter(col("create_time") <= "2020-04-25 18:47:14")
      .withColumn("etl_date", lit("20210101"))

    // todo dwd层数据
    val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")
    val dim_user_info = user_info
      .drop("etl_date")
      .withColumn("birthday", date_format(col("birthday"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val dim_sku_info = sku_info
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val dim_province = base_province
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val dim_region = base_region
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val fact_order_info = order_info
      .drop("etl_date")
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("operate_time")))
      .withColumn("create_time", date_format(col("create_time"), "yyyyMMdd"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val fact_order_detail = order_detail
      .drop("etl_date")
      .withColumn("create_time", date_format(col("create_time"), "yyyyMMdd"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))


    def write_to_hive(df:DataFrame,database:String,table:String): Unit = {
      df.write.format("hive").mode("overwrite").partitionBy("etl_date").saveAsTable(s"${database}.${table}")

      spark.sql(s"show partitions ${database}.${table}").show()
      println(table + "表模拟数据抽取成功！！！！")
    }

    write_to_hive(user_info,"ods","user_info")
    write_to_hive(sku_info,"ods","sku_info")
    write_to_hive(base_province,"ods","base_province")
    write_to_hive(base_region,"ods","base_region")
    write_to_hive(order_info,"ods","order_info")
    write_to_hive(order_detail,"ods","order_detail")

    write_to_hive(dim_user_info,"dwd","dim_user_info")
    write_to_hive(dim_sku_info,"dwd","dim_sku_info")
    write_to_hive(dim_province,"dwd","dim_province")
    write_to_hive(dim_region,"dwd","dim_region")
    write_to_hive(fact_order_info,"dwd","fact_order_info")
    write_to_hive(fact_order_detail,"dwd","fact_order_detail")
  }
}
