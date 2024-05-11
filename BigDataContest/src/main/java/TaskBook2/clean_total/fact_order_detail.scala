package TaskBook2.clean_total

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit, when}

object fact_order_detail {
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

    val result = spark.table("ods_ds_hudi.order_detail")
      .filter(col("etl_date") === "20240428")
      .drop("etl_date")
      .withColumn("create_time", date_format(col("create_time"), "yyyyMMdd"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20240428"))



    writeDataToHudi(result,"fact_order_detail","/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail","dwd_ds_hudi","dwd_modify_time")

    spark.sql("msck repair table dwd_ds_hudi.fact_order_detail")
//
    spark.sql("show partitions dwd_ds_hudi.fact_order_detail").show()


    def writeDataToHudi(dataFrame: DataFrame, hudiTableName: String, hudiTablePath: String,database :String,recombine_key :String): Unit = {
      dataFrame.write.format("org.apache.hudi")
        .option("hoodie.table.name", hudiTableName)
        .option(PRECOMBINE_FIELD.key(), recombine_key)
        // TODO 联合主键
        .option(RECORDKEY_FIELD.key(), "id")
        .option(PARTITIONPATH_FIELD.key(), "etl_date")
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.datasource.write.allowNullValue", "true") // 允许写入空值
        //        .option("hoodie.datasource.hive_sync.support_timestamp","true")
//        .option("hoodie.datasource.hive_sync.enable", "true")
//        .option("hoodie.datasource.hive_sync.mode", "hms")
//        .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://bigdata1:9083")
//        .option("hoodie.datasource.hive_sync.database",database)
//        .option("hoodie.datasource.hive_sync.table", hudiTableName)
//        .option("hoodie.datasource.hive_sync.partition_fields", "etl_date")
//        .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
        .mode("append") // 写入模式，可以是 overwrite 或 append
        .save(hudiTablePath)
    }
  }
}
