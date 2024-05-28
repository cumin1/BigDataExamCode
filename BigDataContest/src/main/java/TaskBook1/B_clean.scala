package TaskBook1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import java.util.Properties

object B_clean {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("taskbook1")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.storeAssignmentPolicy","legacy")
      .getOrCreate()

    val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")

    val user_info = spark.table("ods.user_info").filter(col("etl_date") === "20240526")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val sku_info = spark.table("ods.sku_info").filter(col("etl_date") === "20240526")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val base_province = spark.table("ods.base_province").filter(col("etl_date") === "20240526")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val base_region = spark.table("ods.base_region").filter(col("etl_date") === "20240526")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val order_info = spark.table("ods.order_info").filter(col("etl_date") === "20240526")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val order_detail = spark.table("ods.order_detail").filter(col("etl_date") === "20240526")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val dim_user_info = spark.table("dwd.dim_user_info").drop("etl_date")
    val dim_sku_info = spark.table("dwd.dim_sku_info").drop("etl_date")
    val dim_province = spark.table("dwd.dim_province").drop("etl_date")
    val dim_region = spark.table("dwd.dim_region").drop("etl_date")
//    val fact_order_info = spark.table("dwd.fact_order_info")
//    val fact_order_detail = spark.table("dwd.fact_order_detail")

    val res1 = user_info.union(dim_user_info)
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("create_time")))
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("operate_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("etl_date", date_format(date_sub(current_date(),1), "yyyyMMdd"))

//    res1.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.dim_user_info")

    val res2 = sku_info.union(dim_sku_info)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("create_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("etl_date", date_format(date_sub(current_date(),1), "yyyyMMdd"))

//    res2.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.dim_sku_info")

    val res3 = base_province.union(dim_province)
      .withColumn("create_time", col("create_time").cast(DataTypes.TimestampType))
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("create_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("create_time", col("create_time").cast(DataTypes.StringType))
      .withColumn("etl_date", date_format(date_sub(current_date(),1), "yyyyMMdd"))

//    res3.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.dim_province")

    val res4 = base_region.union(dim_region)
      .withColumn("create_time", col("create_time").cast(DataTypes.TimestampType))
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("create_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("create_time", col("create_time").cast(DataTypes.StringType))
      .withColumn("etl_date", date_format(date_sub(current_date(),1), "yyyyMMdd"))

//    res4.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.dim_region")

    val res5 = order_info
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("create_time")))
      .withColumn("etl_date", date_format(col("create_time"), "yyyyMMdd"))

//    res5.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.fact_order_info")

    val res6 = order_detail
      .withColumn("etl_date", date_format(col("create_time"), "yyyyMMdd"))

//    res6.write.format("hive").mode("append").partitionBy("etl_date").saveAsTable("dwd.fact_order_detail")

    spark.sql("show partitions dwd.dim_user_info").show()
    spark.sql(
      """
        |select id,sku_desc,dwd_insert_user,dwd_modify_time,etl_date
        |from dwd.dim_sku_info where etl_date = 20240526 and id >= 15 and id <= 20 order by id asc
        |""".stripMargin).show()
    spark.sql("select count(*) from dwd.dim_province where etl_date = 20240526").show()
    spark.sql("select count(*) from dwd.dim_region where etl_date = 20240526").show()
    spark.sql("show partitions dwd.fact_order_info").show()
    spark.sql("show partitions dwd.fact_order_detail").show()

    spark.stop()
  }
}
