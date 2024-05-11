package TaskBook2.clean_total

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_NAME}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, desc, lit, row_number}

object dim_sku_info {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("dim_sku_info")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")

    val ods_table = spark.table("ods_ds_hudi.sku_info")
      .filter(col("etl_date") === "20240423")
      .withColumn("create_time", date_format(col("create_time"), "yyyy-MM-dd HH:mm:ss"))
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))

    val dwd_table = spark.table("dwd_ds_hudi.dim_sku_info")
      .drop("etl_date")

    val result = ods_table
      .union(dwd_table)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("create_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("etl_date", lit("20240428"))

    result
      .write
      .format("hudi")
      .mode("append")
      .option(TABLE_NAME.key(),"dim_sku_info")
      .option(RECORDKEY_FIELD.key(),"id")
      .option(PRECOMBINE_FIELD.key(),"dwd_modify_time")
      .option(PARTITIONPATH_FIELD.key(),"etl_date")
      .save("/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")


    spark.sql("""msck repair table dwd_ds_hudi.dim_sku_info""")

    spark.sql("show partitions dwd_ds_hudi.dim_sku_info").show()

  }
}
