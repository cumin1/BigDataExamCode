package TaskBook6

import Utils.Repair_table
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

object B_exact {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书6 exact")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val yesDay = date_format(date_sub(current_date(),1),"yyyyMMdd")
    val nowtime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"
    val prop = new Properties() {
      {
        setProperty("driver", "com.mysql.jdbc.Driver")
        setProperty("user", "root")
        setProperty("password", "123456")
      }
    }

    val user_info = spark.read.jdbc(mysql_url, "user_info", prop)
    val user_info_max_time = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/user_info")
      .select(max(greatest(col("operate_time"), col("create_time")))).first().getTimestamp(0)

    val ods_user_info = user_info
      .withColumn("max_time", greatest(col("operate_time"), col("create_time")))
      .filter(col("max_time") > user_info_max_time)
      .drop("max_time")
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("operate_time")))
      .withColumn("etl_date", lit(yesDay))


    val sku_info = spark.read.jdbc(mysql_url,"sku_info",prop)
    val sku_info_max_time = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/sku_info")
      .select(max(col("create_time"))).first().getTimestamp(0)

    val ods_sku_info = sku_info.filter(col("create_time") > sku_info_max_time)
      .withColumn("etl_date", lit(yesDay))

    val base_province = spark.read.jdbc(mysql_url,"base_province",prop)
    val ods_base_province = base_province.withColumn("create_time",lit(nowtime)).withColumn("etl_date", lit(yesDay))

    val base_region = spark.read.jdbc(mysql_url,"base_region",prop)
    val ods_base_region = base_region.withColumn("create_time",lit(nowtime)).withColumn("etl_date", lit(yesDay))

    val order_info = spark.read.jdbc(mysql_url,"order_info",prop)
    val order_info_max_time = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/order_info")
      .select(max(greatest(col("operate_time"), col("create_time")))).first().getTimestamp(0)

    val ods_order_info = order_info
      .withColumn("max_time", greatest(col("operate_time"), col("create_time")))
      .filter(col("max_time") > order_info_max_time)
      .drop("max_time")
      .withColumn("etl_date", lit(yesDay))

    val order_detail = spark.read.jdbc(mysql_url, "order_detail", prop)
    val order_detail_max_time = spark.read.format("hudi").load("/user/hive/warehouse/ods_ds_hudi.db/order_detail")
      .select(max(col("create_time"))).first().getTimestamp(0)

    val ods_order_detail = order_detail.filter(col("create_time") > order_detail_max_time).withColumn("etl_date", lit(yesDay))



    Utils.HoodieUtils.write(ods_user_info,"etl_date","id","operate_time","user_info","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/user_info",SaveMode.Append)
    Utils.HoodieUtils.write(ods_sku_info,"etl_date","id","create_time","sku_info","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/sku_info",SaveMode.Append)
    Utils.HoodieUtils.write(ods_base_province,"etl_date","id","create_time","base_province","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/base_province",SaveMode.Append)
    Utils.HoodieUtils.write(ods_base_region,"etl_date","id","create_time","base_region","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/base_region",SaveMode.Append)
    Utils.HoodieUtils.write(ods_order_info,"etl_date","id","operate_time","order_info","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/order_info",SaveMode.Append)
    Utils.HoodieUtils.write(ods_order_detail,"etl_date","id","create_time","order_detail","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/order_detail",SaveMode.Append)

    Repair_table.repair_table(spark,"ods_ds_hudi","user_info","/user/hive/warehouse/ods_ds_hudi.db/user_info")
    Repair_table.repair_table(spark,"ods_ds_hudi","sku_info","/user/hive/warehouse/ods_ds_hudi.db/sku_info")
    Repair_table.repair_table(spark,"ods_ds_hudi","base_province","/user/hive/warehouse/ods_ds_hudi.db/base_province")
    Repair_table.repair_table(spark,"ods_ds_hudi","base_region","/user/hive/warehouse/ods_ds_hudi.db/base_region")
    Repair_table.repair_table(spark,"ods_ds_hudi","order_info","/user/hive/warehouse/ods_ds_hudi.db/order_info")
    Repair_table.repair_table(spark,"ods_ds_hudi","order_detail","/user/hive/warehouse/ods_ds_hudi.db/order_detail")

    spark.stop()
  }
}
