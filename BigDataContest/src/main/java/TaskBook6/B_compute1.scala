package TaskBook6

import Utils.Repair_table
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.hudi.DataSourceWriteOptions._

import java.util.UUID

object B_compute1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("任务书6 计算")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val fact_order_info = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .select("final_total_amount","create_time","province_id")
    val dim_province = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/dim_province")
      .select("id","name","region_id")
    val dim_region = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/dim_region")
      .select("id","region_name")

    val uuid_func = spark.udf.register("uuid", () => {
      var uuid = ""
      uuid = UUID.randomUUID().toString
      uuid
    })

    val result = fact_order_info
      .join(dim_province, fact_order_info("province_id") === dim_province("id"))
      .join(dim_region, dim_province("region_id") === dim_region("id"))
      .select(
        dim_province("id") as "province_id",
        dim_province("name") as "province_name",
        dim_region("id") as "region_id",
        dim_region("region_name") as "region_name",
        fact_order_info("final_total_amount") as "amount",
        year(from_unixtime(unix_timestamp(col("create_time"), "yyyyMMdd"))) as "year",
        month(from_unixtime(unix_timestamp(col("create_time"), "yyyyMMdd"))) as "month"
      )
      .groupBy(
        "province_id", "province_name", "region_id", "region_name", "year", "month"
      )
      .agg(
        sum("amount") as "total_amount",
        count("amount") as "total_count"
      )
      .withColumn("sequence", row_number().over(Window.partitionBy("year", "month", "region_id").orderBy(desc("total_amount"))))
      .withColumn("uuid", uuid_func())
      .withColumn("total_amount",col("total_amount").cast(DataTypes.DoubleType))
      .select(
        "uuid", "province_id", "province_name", "region_id", "region_name", "total_amount", "total_count", "sequence", "year", "month"
      )

    result.show()
    result.createTempView("province_consumption_day_aggr")
    spark.sql(
      """select uuid,province_id,province_name,region_id,region_name,cast(total_amount as bigint),total_count,sequence,year,month
        |from province_consumption_day_aggr order by
        |total_count desc,total_amount desc,province_id desc
        |limit 5""".stripMargin).show()

    result.write.format("hudi").mode("overwrite")
      .option("hoodie.table.name","province_consumption_day_aggr")
      .option(PARTITIONPATH_FIELD.key(),"year,month")
      .option(RECORDKEY_FIELD.key(),"uuid")
      .option(PRECOMBINE_FIELD.key(),"total_count")
      .option("hoodie.datasource.write.hive_style_partitioning","true")
      .save("/user/hive/warehouse/dws_ds_hudi.db/province_consumption_day_aggr")

    Repair_table.repair_table(spark,"dws_ds_hudi","province_consumption_day_aggr","/user/hive/warehouse/dws_ds_hudi.db/province_consumption_day_aggr")

  }
}
