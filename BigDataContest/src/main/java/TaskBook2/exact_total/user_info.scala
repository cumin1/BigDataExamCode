package TaskBook2.exact_total

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_format, date_sub, from_unixtime, greatest, lit, max, unix_timestamp, when}
import org.apache.hudi.DataSourceWriteOptions._

import java.util.Properties

object user_info {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书2 抽取")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val prop = new Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "123456")
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"

    val yesDay = date_format(date_sub(current_date(), 1), "yyyyMMdd")
//    val nowTime = date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")


    val mysql_table = spark.read.jdbc(mysql_url, "user_info", prop)
    val ods_table = spark.table("ods_ds_hudi.user_info")
    mysql_table.show()
    ods_table.show()

    val max_time = ods_table.select(max(greatest(col("create_time"), col("operate_time")))).first().getTimestamp(0)

    println(max_time)

    val result = mysql_table
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("operate_time")))
      .withColumn("max_time", greatest(col("create_time"), col("operate_time")))
      .filter(col("max_time") > max_time)
      .drop("max_time")
      .withColumn("etl_date", lit(yesDay))

    result.show()

    write_to_hudi(result,"user_info","operate_time")

    def write_to_hudi(dataframe :DataFrame,table_name :String, precombine_key :String): Unit = {
      dataframe
        .write
        .format("hudi")
        .mode("append")
        .option("hoodie.table.name",table_name)
        .option(RECORDKEY_FIELD.key(),"id")
        .option(PRECOMBINE_FIELD.key(),precombine_key)
        .option(PARTITIONPATH_FIELD.key(),"etl_date")
//        .option("hoodie.datasource.hive_sync.enable","true")
//        .option("hoodie.datasource.hive_sync.use_jdbc","false")
//        .option("hoodie.datasource.hive_sync.mode","hms")
//        .option("hoodie.datasource.hive_sync.metastore.uris","thrift://bigdata1:9083")
//        .option("hoodie.datasource.hive_sync.database","ods_ds_hudi")
//        .option("hoodie.datasource.hive_sync.table",table_name)
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.datasource.write.allowNullValue","true")
//        .option("hoodie.datasource.hive_sync.support_timestamp","true")
//        .option("hoodie.datasource.hive_sync.partition_fields", "etl_date")
//        .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
        .save("/user/hive/warehouse/ods_ds_hudi.db/" + table_name)
    }


    spark.sql(
      """
        |msck repair table ods_ds_hudi.user_info
        |""".stripMargin)

    spark.sql("show partitions ods_ds_hudi.user_info").show()


    spark.stop()
  }
}
