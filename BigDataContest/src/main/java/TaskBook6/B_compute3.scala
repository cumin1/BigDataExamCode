package TaskBook6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object B_compute3 {
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

    val source = fact_order_info
      .join(dim_province, fact_order_info("province_id") === dim_province("id"))
      .join(dim_region, dim_province("region_id") === dim_region("id"))
      .withColumn("year", year(from_unixtime(unix_timestamp(col("create_time"), "yyyyMMdd"))))
      .filter(col("year") === 2020)
      .select(
        dim_province("id") as "provinceid",
        dim_province("name") as "provincename",
        dim_region("id") as "regionid",
        dim_region("region_name") as "regionname",
        fact_order_info("final_total_amount") as "amount"
      )

    source.show()

    val df1 = source
      .groupBy("provinceid","provincename","regionid","regionname")
      .agg(
        expr("percentile(amount,0.5)") as "provincemedian"
      )

    val df2 = source
      .groupBy("regionid")
      .agg(
        expr("percentile(amount,0.5)") as "regionmedian"
      )
      .withColumnRenamed("regionid","rid")

    val result = df1
      .join(df2, df1("regionid") === df2("rid"))
      .select(
        df1("provinceid") as "provinceid",
        df1("provincename") as "provincename",
        df1("regionid") as "regionid",
        df1("regionname") as "regionname",
        df1("provincemedian") as "provincemedian",
        df2("regionmedian") as "regionmedian"
      )

    result.show()
    result.createTempView("nationmedian")
    spark.sql("select * from nationmedian order by regionid asc,provinceid asc limit 5").show()

  }
}
