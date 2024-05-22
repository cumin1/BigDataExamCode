package moni_1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.expressions.Window

import java.util
import java.util.{Properties, UUID}
object B_compute_3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val dim_province = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_province")
      .select("id", "name", "region_id")
    val dim_region = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_region")
      .select("id", "region_name")
    val fact_order_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .select("final_total_amount", "create_time", "province_id")

    val source = fact_order_info
      .join(dim_province, fact_order_info("province_id") === dim_province("id"))
      .join(dim_region, dim_province("region_id") === dim_region("id"))
      .select(
        dim_province("id") as "provinceid",
        dim_province("name") as "provincename",
        dim_region("id") as "regionid",
        dim_region("region_name") as "regionname",
        fact_order_info("final_total_amount") as "amount"
      )

    val df1 = source.groupBy("provinceid", "provincename", "regionid", "regionname")
      .agg(
        expr("percentile(amount,0.5)") as "provincemedian"
      )

    val df2 = source.groupBy("regionid")
      .agg(
        expr("percentile(amount,0.5)") as "regionmedian"
      )


    val result = df1.join(df2, Seq("regionid"))
      .select("provinceid", "provincename", "regionid", "regionname", "provincemedian", "regionmedian")

    result.show(5)

    val properties = new Properties() {
      {
        setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        setProperty("user", "default")
        setProperty("password", "123456")
      }
    }

    result.write.mode("append").jdbc("jdbc:clickhouse://bigdata1:8123/shtd_result?useSSL=false","nationmedian",properties)
// select * from nationmedian order by provinceid asc limit 5;
    spark.stop()
  }
}
