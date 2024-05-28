package TaskBook1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

object B_exact {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("taskbook1")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val properties = new Properties() {
      {
        setProperty("driver", "com.mysql.jdbc.Driver")
        setProperty("user", "root")
        setProperty("password", "123456")
      }
    }
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"

    val user_info = spark.read.jdbc(mysql_url, "user_info", properties)
    val sku_info = spark.read.jdbc(mysql_url, "sku_info", properties)
    val base_province = spark.read.jdbc(mysql_url, "base_province", properties)
    val base_region = spark.read.jdbc(mysql_url, "base_region", properties)
    val order_info = spark.read.jdbc(mysql_url, "order_info", properties)
    val order_detail = spark.read.jdbc(mysql_url, "order_detail", properties)

    val max_time_user_info = spark.table("ods.user_info")
      .select(max(greatest(col("operate_time"),col("create_time"))))
      .first().getTimestamp(0)

    val ods_user_info = user_info
      .withColumn("max_time", greatest(col("operate_time"), col("create_time")))
      .filter(col("max_time") > max_time_user_info)
      .drop("max_time")
      .withColumn("etl_date", date_format(date_sub(current_date(),1), "yyyyMMdd"))

//    ods_user_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.user_info")

    val max_time_sku_info = spark.table("ods.sku_info")
      .select(max("create_time"))
      .first().getTimestamp(0)

    val ods_sku_info = sku_info
      .filter(col("create_time") > max_time_sku_info)
      .withColumn("etl_date", date_format(date_sub(current_date(), 1), "yyyyMMdd"))

//    ods_sku_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.sku_info")

    val max_id_base_province = spark.table("ods.base_province").select(max("id")).first().getLong(0)

    val ods_base_province = base_province.filter(col("id") > max_id_base_province)
      .withColumn("create_time",date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", date_format(date_sub(current_date(), 1), "yyyyMMdd"))

//    ods_base_province.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.base_province")

    val max_id_base_region = spark.table("ods.base_region")
      .withColumn("id",col("id").cast("int"))
      .select(max("id")).first().getInt(0)

    val ods_base_region = base_region
      .withColumn("id",col("id").cast("int"))
      .filter(col("id") > max_id_base_region)
      .withColumn("create_time",date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", date_format(date_sub(current_date(), 1), "yyyyMMdd"))

    ods_base_region.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.base_region")

    val max_time_order_info = spark.read.table("ods.order_info")
      .select(max(greatest(col("operate_time"), col("create_time"))))
      .first().getTimestamp(0)

    val ods_order_info = order_info
      .withColumn("max_time", greatest(col("operate_time"), col("create_time")))
      .filter(col("max_time") > max_time_order_info)
      .drop("max_time")
      .withColumn("etl_date", date_format(date_sub(current_date(), 1), "yyyyMMdd"))

    ods_order_info.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.order_info")

    val max_time_order_detail = spark.table("ods.order_detail")
      .select(max("create_time")).first().getTimestamp(0)

    val ods_order_detail = order_detail
      .filter(col("create_time") > max_time_order_detail)
      .withColumn("etl_date", date_format(date_sub(current_date(), 1), "yyyyMMdd"))

    ods_order_detail.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("ods.order_detail")

    spark.sql("show partitions ods.user_info").show()
    spark.sql("show partitions ods.sku_info").show()
    spark.sql("show partitions ods.base_province").show()
    spark.sql("show partitions ods.base_region").show()
    spark.sql("show partitions ods.order_info").show()
    spark.sql("show partitions ods.order_detail").show()

    spark.stop()
  }
}
