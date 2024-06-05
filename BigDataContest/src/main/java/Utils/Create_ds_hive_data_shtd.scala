package Utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Create_ds_hive_data_shtd {
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

    val ods_user_info = spark.read.parquet("/tmp/hive_data/hive_ods_user_info_partitionis20210102")
    val ods_sku_info = spark.read.parquet("/tmp/hive_data/hive_ods_sku_info_partitionis20210102")
    val ods_order_info = spark.read.parquet("/tmp/hive_data/hive_ods_order_info_partitionis20210102")
    val ods_order_detail = spark.read.parquet("/tmp/hive_data/hive_ods_order_detail_partitionis20210102")

    val dim_user_info = spark.read.parquet("/tmp/hive_data/hive_dim_user_info_partitionis20210102")
    val dim_sku_info = spark.read.parquet("/tmp/hive_data/hive_dim_sku_info_partitionis20210102")
    val fact_order_info = spark.read.parquet("/tmp/hive_data/hive_fact_order_info_autopartition")
    val fact_order_detail = spark.read.parquet("/tmp/hive_data/hive_fact_order_detail_autopartition")

    Array(ods_user_info,ods_sku_info,ods_order_info,ods_order_detail,dim_user_info,dim_sku_info,fact_order_info,fact_order_detail).foreach(df =>{
      df.show()
    })

//    ods_user_info.write.format("hive").partitionBy("etl_date").mode("overwrite").saveAsTable("ods.user_info")
//    ods_sku_info.write.format("hive").partitionBy("etl_date").mode("overwrite").saveAsTable("ods.sku_info")
//    spark.sql(
//      """
//        |create table if not exists ods.base_province(
//        |id bigint,
//        |name string,
//        |region_id string,
//        |area_code string,
//        |iso_code string,
//        |create_time datetime
//        |)
//        |partitioned by (etl_date string);
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |create table ods.base_region(
//        |id bigint,
//        |region_name string,
//        |create_time datetime
//        |)
//        |partitioned by (etl_date string);
//        |""".stripMargin)


//    ods_order_info.write.format("hive").partitionBy("etl_date").mode("overwrite").saveAsTable("ods.order_info")
//    ods_order_detail.write.format("hive").partitionBy("etl_date").mode("overwrite").saveAsTable("ods.order_detail")

//
//    dim_user_info.write.format("hive").partitionBy("etl_date").mode("overwrite").saveAsTable("dwd.dim_user_info")
//    dim_sku_info.write.format("hive").partitionBy("etl_date").mode("overwrite").saveAsTable("dwd.dim_sku_info ")
    fact_order_info.write.format("hive").partitionBy("etl_date").mode("overwrite").saveAsTable("dwd.fact_order_info ")
    fact_order_detail.write.format("hive").partitionBy("etl_date").mode("overwrite").saveAsTable("dwd.fact_order_detail ")



  }
}
