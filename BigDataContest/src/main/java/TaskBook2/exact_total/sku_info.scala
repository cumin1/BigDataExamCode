package TaskBook2.exact_total


import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import java.util.Properties

object sku_info {
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

    val mysql_table = spark.read.jdbc(mysql_url,"sku_info",prop)

    val ods_table = spark.table("ods_ds_hudi.sku_info")

    val max_time = ods_table.select(max("create_time")).first().getTimestamp(0)

    val result = mysql_table
      .filter(col("create_time") > max_time)
      .withColumn("price",col("price").cast(DataTypes.DoubleType))
      .withColumn("etl_date", lit(yesDay))

    write_to_hudi(result,"sku_info","create_time")

    def write_to_hudi(dataframe :DataFrame,table_name :String, precombine_key :String): Unit = {
      dataframe
        .write
        .format("hudi")
        .mode("append")
        .option("hoodie.table.name",table_name)
        .option(RECORDKEY_FIELD.key(),"id")
        .option(PRECOMBINE_FIELD.key(),precombine_key)
        .option(PARTITIONPATH_FIELD.key(),"etl_date")
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.datasource.write.allowNullValue","true")
        .save("/user/hive/warehouse/ods_ds_hudi.db/" + table_name)
    }


    spark.sql(
      """
        |msck repair table ods_ds_hudi.sku_info
        |""".stripMargin)

    spark.sql("show partitions ods_ds_hudi.sku_info").show()

    spark.stop()

  }
}
