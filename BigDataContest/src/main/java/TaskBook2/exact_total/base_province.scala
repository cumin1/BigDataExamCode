package TaskBook2.exact_total

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._

import java.util.Properties

object base_province {
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

    val nowTime = date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")

    val mysql_table = spark.read.jdbc("jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false", "base_province", new Properties {
      {
        setProperty("user", "root")
        setProperty("password", "123456")
        setProperty("driver", "com.mysql.jdbc.Driver")
      }
    }
    )


    val ods_table = spark.table("ods_ds_hudi.base_province")
    ods_table.show(5)
    import spark.implicits._
    val yesDay = date_format(date_sub(current_date(), 1), "yyyyMMdd")
    val max_id = ods_table.select(max("id")).take(1)(0)(0)
    val result = mysql_table
      .filter(col("id") > max_id)
      .withColumn("create_time",lit(nowTime))
      .withColumn("etl_date", lit(yesDay))


    result
      .write
      .format("hudi")
      .mode("append")
      .option(TABLE_NAME.key(),"base_province")
      .option(RECORDKEY_FIELD.key(),"id")
      .option(PRECOMBINE_FIELD.key(),"create_time")
      .option(PARTITIONPATH_FIELD.key,"etl_date")
      .save("/user/hive/warehouse/ods_ds_hudi.db/base_province")


    spark.sql("msck repair table ods_ds_hudi.base_province")

    spark.sql("show partitions ods_ds_hudi.base_province").show()
  }
}
