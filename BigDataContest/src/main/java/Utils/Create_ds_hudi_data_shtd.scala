package Utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object Create_ds_hudi_data_shtd {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("模拟增量数据")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("spark.sql.parquet.writeLegacyFormat","true")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val hudi_ods_user_info = spark.read.parquet("/tmp/hive_data/hudi_ods_user_info_partitionis20210102")
      .withColumn("operate_time",when(col("operate_time").isNull,col("create_time")).otherwise(col("operate_time")))
//      .withColumn("id",col("id").cast(DataTypes.LongType))

    val hudi_ods_sku_info = spark.read.parquet("/tmp/hive_data/hudi_ods_sku_info_partitionis20210102")
//      .withColumn("id",col("id").cast(DataTypes.LongType))

    val hudi_ods_order_info = spark.read.parquet("/tmp/hive_data/hudi_ods_order_info_partitionis20210102")
//      .withColumn("id",col("id").cast(DataTypes.LongType))

    val hudi_ods_order_detail = spark.read.parquet("/tmp/hive_data/hudi_ods_order_detail_partitionis20210102")
//      .withColumn("id",col("id").cast(DataTypes.LongType))

    val hudi_dim_user_info = spark.read.parquet("/tmp/hive_data/hudi_dim_user_info_partitionis20210102")
//      .withColumn("id",col("id").cast(DataTypes.LongType))
      .withColumn("operate_time",when(col("operate_time").isNull,col("create_time")).otherwise(col("operate_time")))

    val hudi_dim_sku_info = spark.read.parquet("/tmp/hive_data/hudi_dim_sku_info_partitionis20210102")
//      .withColumn("id",col("id").cast(DataTypes.LongType))

    hudi_ods_user_info.printSchema()
    hudi_ods_sku_info.printSchema()
    hudi_ods_order_info.printSchema()
    hudi_ods_order_detail.printSchema()
    hudi_dim_user_info.printSchema()
    hudi_dim_sku_info.printSchema()

    hudi_ods_user_info.show()
    hudi_ods_sku_info.show()
    hudi_ods_order_info.show()
    hudi_ods_order_detail.show()
    hudi_dim_user_info.show()
    hudi_dim_sku_info.show()

    HoodieUtils.write(hudi_ods_user_info,"etl_date","id","operate_time","user_info","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/user_info",SaveMode.Overwrite)
    HoodieUtils.write(hudi_ods_sku_info,"etl_date","id","create_time","sku_info","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/sku_info",SaveMode.Overwrite)
//    HoodieUtils.write(hudi_ods_order_info,"etl_date","id","operate_time","order_info","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/order_info",SaveMode.Overwrite)
//    HoodieUtils.write(hudi_ods_order_detail,"etl_date","id","create_time","order_detail","ods_ds_hudi","/user/hive/warehouse/ods_ds_hudi.db/order_detail",SaveMode.Overwrite)
    HoodieUtils.write(hudi_dim_user_info,"etl_date","id","operate_time","dim_user_info","dwd_ds_hudi","/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info",SaveMode.Overwrite)
    HoodieUtils.write(hudi_dim_sku_info,"etl_date","id","dwd_modify_time","dim_sku_info","dwd_ds_hudi","/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info",SaveMode.Overwrite)

    Repair_table.repair_table(spark,"ods_ds_hudi","user_info","/user/hive/warehouse/ods_ds_hudi.db/user_info")
    Repair_table.repair_table(spark,"ods_ds_hudi","sku_info","/user/hive/warehouse/ods_ds_hudi.db/sku_info")
//    Repair_table.repair_table(spark,"ods_ds_hudi","order_info","/user/hive/warehouse/ods_ds_hudi.db/order_info")
//    Repair_table.repair_table(spark,"ods_ds_hudi","order_detail","/user/hive/warehouse/ods_ds_hudi.db/order_detail")
    Repair_table.repair_table(spark,"dwd_ds_hudi","dim_user_info","/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
    Repair_table.repair_table(spark,"dwd_ds_hudi","dim_sku_info","/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")

    Repair_table.show_partition(spark,"ods_ds_hudi","user_info")
    Repair_table.show_partition(spark,"ods_ds_hudi","sku_info")
//    Repair_table.show_partition(spark,"ods_ds_hudi","order_info")
//    Repair_table.show_partition(spark,"ods_ds_hudi","order_detail")
    Repair_table.show_partition(spark,"dwd_ds_hudi","dim_user_info")
    Repair_table.show_partition(spark,"dwd_ds_hudi","dim_sku_info")
  }
}
