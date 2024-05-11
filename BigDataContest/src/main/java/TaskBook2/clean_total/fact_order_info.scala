package TaskBook2.clean_total

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._

object fact_order_info {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("fact_order_info")
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

    val result = spark.table("ods_ds_hudi.order_info")
      .filter(col("etl_date") === "20240428")
      .drop("etl_date")
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("operate_time")))
      .withColumn("create_time", date_format(col("create_time"), "yyyyMMdd"))
      .withColumn("operate_time", date_format(col("operate_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("expire_time", date_format(col("expire_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20240428"))



    writeDataToHudi(result,"fact_order_info","/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info","dwd_ds_hudi","operate_time")

    spark.sql("msck repair table dwd_ds_hudi.fact_order_info")

    spark.sql("show partitions dwd_ds_hudi.fact_order_info").show()


    def writeDataToHudi(dataFrame: DataFrame, hudiTableName: String, hudiTablePath: String,database :String,recombine_key :String): Unit = {
      dataFrame.write.format("org.apache.hudi")
        .option("hoodie.table.name", hudiTableName)
        .option(PRECOMBINE_FIELD.key(), recombine_key)
        // TODO 联合主键
        .option(RECORDKEY_FIELD.key(), "id")
        .option(PARTITIONPATH_FIELD.key(), "etl_date")
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.datasource.write.allowNullValue", "true") // 允许写入空值
        .mode("append") // 写入模式，可以是 overwrite 或 append
        .save(hudiTablePath)
    }

  }
}
