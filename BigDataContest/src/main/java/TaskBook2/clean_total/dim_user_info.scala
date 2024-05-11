package TaskBook2.clean_total

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._

object dim_user_info {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("user_info 清洗")
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

    val ods_table = spark.table("ods_ds_hudi.user_info")
      .filter(col("etl_date") === "20240423")
      .withColumn("birthday", date_format(col("birthday"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("create_time", date_format(col("create_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("operate_time", date_format(col("operate_time"), "yyyy-MM-dd HH:mm:ss"))
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))


    val dwd_table = spark.table("dwd_ds_hudi.dim_user_info")
      .drop("etl_date")

    val result = ods_table
      .union(dwd_table)
      .withColumn("row", row_number().over(Window.partitionBy("id").orderBy(desc("operate_time"))))
      .filter(col("row") === 1)
      .drop("row")
      .withColumn("etl_date", lit("20240428"))

    result
      .write
      .format("hudi")
      .mode("append")
      .option(TABLE_NAME.key(),"dim_user_info")
      .option(RECORDKEY_FIELD.key(),"id")
      .option(PRECOMBINE_FIELD.key(),"operate_time")
      .option(PARTITIONPATH_FIELD.key(),"etl_date")
      .save("/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")


    spark.sql("""msck repair table dwd_ds_hudi.dim_user_info""")

    spark.sql("show partitions dwd_ds_hudi.dim_user_info").show()

  }
}
