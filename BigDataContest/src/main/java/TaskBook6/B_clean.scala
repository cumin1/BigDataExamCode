package TaskBook6

import Utils.{HoodieUtils, Repair_table}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object B_clean {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("任务书6 清洗")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")
    val yesDay = date_format(date_sub(current_date(),1),"yyyyMMdd")

    val user_info = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/user_info").where(col("etl_date") === "20240505")
      .drop("etl_date")
      .withColumn("birthday",date_format(col("birthday"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(nowTime))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(nowTime))
    val dwd_user_info = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
      .drop("etl_date")

    val dim_user_info = user_info.union(dwd_user_info)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("operate_time"))))
      .filter(col("row") === 1).drop("row").withColumn("etl_date", lit(yesDay))


    val sku_info = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/sku_info").where(col("etl_date") === "20240505")
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val dwd_sku_info = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")
      .drop("etl_date")

    val dim_sku_info = sku_info.union(dwd_sku_info)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("create_time"))))
      .filter(col("row") === 1).drop("row").withColumn("etl_date", lit(yesDay))


    val base_province = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/base_province").where(col("etl_date") === "20240505")

    val dim_province = base_province.drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit(yesDay))

    val base_region = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/base_region").where(col("etl_date") === "20240505")

    val dim_region = base_region.drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit(yesDay))


    val order_info = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/order_info").where(col("etl_date") === "20240505")
    val fact_order_info = order_info.drop("etl_date")
      .withColumn("create_time",date_format(col("create_time"),"yyyyMMdd"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit(yesDay))

    val order_detail = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/order_detail").where(col("etl_date") === "20240505")
    val fact_order_detail = order_detail.drop("etl_date")
    .withColumn("create_time",date_format(col("create_time"),"yyyyMMdd"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit(yesDay))


    HoodieUtils.write(dim_user_info,"etl_date","id","operate_time","dim_user_info","dwd_ds_hudi","/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info",SaveMode.Append)
    HoodieUtils.write(dim_sku_info,"etl_date","id","dwd_modify_time","dim_sku_info","dwd_ds_hudi","/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info",SaveMode.Append)
    HoodieUtils.write(dim_province,"etl_date","id","dwd_modify_time","dim_province","dwd_ds_hudi","/user/hive/warehouse/dwd_ds_hudi.db/dim_province",SaveMode.Append)
    HoodieUtils.write(dim_region,"etl_date","id","dwd_modify_time","dim_region","dwd_ds_hudi","/user/hive/warehouse/dwd_ds_hudi.db/dim_region",SaveMode.Append)
    HoodieUtils.write(fact_order_info,"etl_date","id","operate_time","fact_order_info","dwd_ds_hudi","/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info",SaveMode.Append)
    HoodieUtils.write(fact_order_detail,"etl_date","id","dwd_modify_time","fact_order_detail","dwd_ds_hudi","/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail",SaveMode.Append)

    Repair_table.repair_table(spark,"dwd_ds_hudi","dim_user_info","/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
    Repair_table.repair_table(spark,"dwd_ds_hudi","dim_sku_info","/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")
    Repair_table.repair_table(spark,"dwd_ds_hudi","dim_province","/user/hive/warehouse/dwd_ds_hudi.db/dim_province")
    Repair_table.repair_table(spark,"dwd_ds_hudi","dim_region","/user/hive/warehouse/dwd_ds_hudi.db/dim_region")
    Repair_table.repair_table(spark,"dwd_ds_hudi","fact_order_info","/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
    Repair_table.repair_table(spark,"dwd_ds_hudi","fact_order_detail","/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")

    spark.stop()
  }
}
