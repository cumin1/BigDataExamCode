package TaskBook2.exact_total


import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_NAME}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

object order_detail {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("抽取 base_province")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()


    val mysql_table = spark.read.jdbc("jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false", "order_detail", new Properties {
      {
        setProperty("user", "root")
        setProperty("password", "123456")
        setProperty("driver", "com.mysql.jdbc.Driver")
      }
    }
    )

    val max_time = spark.table("ods_ds_hudi.order_detail")
      .select(max("create_time"))
      .first().getTimestamp(0)
    val yesDay = date_format(date_sub(current_date(), 1), "yyyyMMdd")

    val result = mysql_table
      .filter(col("create_time") > max_time)
      .withColumn("etl_date", lit(yesDay))

    result
      .write
      .format("hudi")
      .mode("append")
      .option(TABLE_NAME.key(),"order_detail")
      .option(RECORDKEY_FIELD.key(),"id")
      .option(PRECOMBINE_FIELD.key(),"create_time")
      .option(PARTITIONPATH_FIELD.key,"etl_date")
      .save("/user/hive/warehouse/ods_ds_hudi.db/order_detail")

    spark.sql("msck repair table ods_ds_hudi.order_detail")
    spark.sql("show partitions ods_ds_hudi.order_detail").show()

  }
}
