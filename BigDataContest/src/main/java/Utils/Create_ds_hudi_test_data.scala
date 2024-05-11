package Utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_format, lit, when}
import org.apache.hudi.DataSourceWriteOptions._

import java.util.Properties

object Create_ds_hudi_test_data {
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

    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","123456")
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"

    // todo hudi的模拟数据 采用从mysql抽取一部分的方法模拟
    // todo ods层数据
    val user_info = spark.read.jdbc(mysql_url, "user_info", prop)
      .withColumn("operate_time",when(col("operate_time").isNull,col("create_time")).otherwise(col("operate_time")))
      .filter(col("operate_time") === "2020-04-26 00:12:14")
      .withColumn("etl_date",lit("20210101"))

    val sku_info = spark.read.jdbc(mysql_url, "sku_info", prop)
      .filter(col("create_time") === "2021-01-01 12:21:13")
      .withColumn("etl_date", lit("20210101"))

    val base_province = spark.read.jdbc(mysql_url, "base_province", prop)
      .filter(col("id") <= 12)
      .withColumn("create_time",date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", lit("20210101"))

    val base_region = spark.read.jdbc(mysql_url, "base_region", prop)
      .filter(col("id") <= 2)
      .withColumn("create_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("etl_date", lit("20210101"))

    val order_info = spark.read.jdbc(mysql_url, "order_info", prop)
      .filter(col("operate_time") === "2020-04-25 18:47:14")
      .withColumn("etl_date", lit("20210101"))

    val order_detail = spark.read.jdbc(mysql_url, "order_detail", prop)
      .filter(col("create_time") <= "2020-04-25 18:47:14")
      .withColumn("etl_date", lit("20210101"))

    // todo dwd层数据
    val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")
    val dim_user_info = user_info
      .drop("etl_date")
      .withColumn("birthday", date_format(col("birthday"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val dim_sku_info = sku_info
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val dim_province = base_province
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val dim_region = base_region
      .drop("etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val fact_order_info = order_info
      .drop("etl_date")
      .withColumn("operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("operate_time")))
      .withColumn("create_time", date_format(col("create_time"), "yyyyMMdd"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    val fact_order_detail = order_detail
      .drop("etl_date")
      .withColumn("create_time", date_format(col("create_time"), "yyyyMMdd"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etl_date", lit("20210101"))

    def write_to_hudi(df:DataFrame,databasename:String,huditablename:String,recordkey:String,
                      precombinekey:String, partitionkey:String,savepath:String): Unit = {
      df.write.format("hudi").mode("overwrite")
        .option("hoodie.table.name",huditablename)
        .option(RECORDKEY_FIELD.key(),recordkey)
        .option(PRECOMBINE_FIELD.key(),precombinekey)
        .option(PARTITIONPATH_FIELD.key(),partitionkey)
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.datasource.write.allowNullValue", "true") // 允许写入空值
        .save(savepath)

      spark.sql(s"create table IF NOT EXISTS ${databasename}.${huditablename} using hudi location '${savepath}'")
      spark.sql(s"msck repair table ${databasename}.${huditablename}")
      spark.sql(s"show partitions ${databasename}.${huditablename}").show()
      println(s"${huditablename}模拟数据抽取完成！！！！")
    }

    write_to_hudi(user_info,"ods_ds_hudi","user_info","id","operate_time","etl_date","/user/hive/warehouse/ods_ds_hudi.db/user_info")
    write_to_hudi(sku_info,"ods_ds_hudi","sku_info","id","create_time","etl_date","/user/hive/warehouse/ods_ds_hudi.db/sku_info")
    write_to_hudi(base_province,"ods_ds_hudi","base_province","id","create_time","etl_date","/user/hive/warehouse/ods_ds_hudi.db/base_province")
    write_to_hudi(base_region,"ods_ds_hudi","base_region","id","create_time","etl_date","/user/hive/warehouse/ods_ds_hudi.db/base_region")
    write_to_hudi(order_info,"ods_ds_hudi","order_info","id","operate_time","etl_date","/user/hive/warehouse/ods_ds_hudi.db/order_info")
    write_to_hudi(order_detail,"ods_ds_hudi","order_detail","id","create_time","etl_date","/user/hive/warehouse/ods_ds_hudi.db/order_detail")

    write_to_hudi(dim_user_info,"dwd_ds_hudi","dim_user_info","id","operate_time","etl_date","/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
    write_to_hudi(dim_sku_info,"dwd_ds_hudi","dim_sku_info","id","dwd_modify_time","etl_date","/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")
    write_to_hudi(dim_province,"dwd_ds_hudi","dim_province","id","dwd_modify_time","etl_date","/user/hive/warehouse/dwd_ds_hudi.db/dim_province")
    write_to_hudi(dim_region,"dwd_ds_hudi","dim_region","id","dwd_modify_time","etl_date","/user/hive/warehouse/dwd_ds_hudi.db/dim_region")
    write_to_hudi(fact_order_info,"dwd_ds_hudi","fact_order_info","id","operate_time","etl_date","/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
    write_to_hudi(fact_order_detail,"dwd_ds_hudi","fact_order_detail","id","dwd_modify_time","etl_date","/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
  }
}
