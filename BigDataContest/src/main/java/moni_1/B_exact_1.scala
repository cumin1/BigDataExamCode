package moni_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._

import java.util.Properties

object B_exact_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("数据抽取")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val prop = new Properties() {
      {
        setProperty("user", "root")
        setProperty("password", "123456")
        setProperty("driver", "com.mysql.jdbc.Driver")
      }
    }
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"

    val user_info = spark.read.jdbc(mysql_url, "user_info", prop)


    val user_info_max = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/user_info")
      .select(max(greatest(col("operate_time"), col("create_time")))).first().getTimestamp(0)

    val ods_user_info = user_info
      .withColumn("operate_time",
        when(col("operate_time").isNull, col("create_time"))
          .otherwise(col("operate_time"))
      )
      .withColumn("max_time", greatest(col("operate_time"), col("create_time")))
      .filter(col("max_time") > user_info_max)
      .drop("max_time")
      .withColumn("etl_date", lit("20240521"))


    ods_user_info
      .write.format("hudi").mode("append")
      .option("hoodie.table.name","user_info")
      .option(PARTITIONPATH_FIELD.key(),"etl_date")
      .option(PRECOMBINE_FIELD.key(),"operate_time")
      .option(RECORDKEY_FIELD.key(),"id")
      .option("hoodie.datasource.write.hive_style_partitioning","true")
      .save("hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/user_info")

    spark.sql("msck repair table ods_ds_hudi.user_info")
    spark.sql("show partitions ods_ds_hudi.user_info").show()

  }
}
