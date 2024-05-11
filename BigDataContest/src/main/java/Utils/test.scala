package Utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object test {
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

    val hudi_ods_user_info = spark.read.parquet("/tmp/hive_data/hudi_ods_user_info_partitionis20210102")
      .withColumn("operate_time",when(col("operate_time").isNull,col("create_time")).otherwise(col("operate_time")))
    val hudi_ods_sku_info = spark.read.parquet("/tmp/hive_data/hudi_ods_sku_info_partitionis20210102")
    val hudi_ods_order_info = spark.read.parquet("/tmp/hive_data/hudi_ods_order_info_partitionis20210102")
    val hudi_ods_order_detail = spark.read.parquet("/tmp/hive_data/hudi_ods_order_detail_partitionis20210102")
    val hudi_dim_user_info = spark.read.parquet("/tmp/hive_data/hudi_dim_user_info_partitionis20210102")
      .withColumn("operate_time",when(col("operate_time").isNull,col("create_time")).otherwise(col("operate_time")))
    val hudi_dim_sku_info = spark.read.parquet("/tmp/hive_data/hudi_dim_sku_info_partitionis20210102")

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

    hudi_ods_user_info.write.format("csv").save("file:///tmp/test_data_csv/hudi_ods_user_info.csv")
    hudi_ods_sku_info.write.format("csv").save("file:///tmp/test_data_csv/hudi_ods_sku_info.csv")
    hudi_ods_order_info.write.format("csv").save("file:///tmp/test_data_csv/hudi_ods_order_info.csv")
    hudi_ods_order_detail.write.format("csv").save("file:///tmp/test_data_csv/hudi_ods_order_detail.csv")
    hudi_dim_user_info.write.format("csv").save("file:///tmp/test_data_csv/hudi_dim_user_info.csv")
    hudi_dim_sku_info.write.format("csv").save("file:///tmp/test_data_csv/hudi_dim_sku_info.csv")


  }
}
