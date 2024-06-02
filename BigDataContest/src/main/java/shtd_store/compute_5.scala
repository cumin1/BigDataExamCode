package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, desc, month, row_number, sum, year}

import java.util.Properties

object compute_5 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)


    val spark = SparkSession
      .builder()
      .appName("任务书1 指标计算")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy","LEGACY")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()


    val url = "jdbc:mysql://bigdata1:3306/shtd_result"
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    prop.put("driver","com.mysql.cj.jdbc.Driver")

    // (1)
    val dim_province = spark.table("dwd.dim_province")
    val dim_region = spark.table("dwd.dim_region")
    val fact_order_info = spark.table("dwd.fact_order_info")


    val result1 = fact_order_info
      .join(dim_province,fact_order_info("province_id") === dim_province("id"))
      .join(dim_region,dim_province("region_id") === dim_region("id"))
      .select(
        dim_province("id") as "province_id",
        dim_province("name") as "province_name",
        dim_region("id") as "region_id",
        dim_region("region_name") as "region_name",
        fact_order_info("final_total_amount") as "amount",
        year(fact_order_info("create_time")) as "year",
        month(fact_order_info("create_time")) as "month"
      )
      .groupBy("province_id","province_name","region_id","region_name","year","month")
      .agg(
        sum("amount") as "total_amount",
        count("amount") as "total_count"
      )
      .withColumn("sequence", row_number().over(Window.partitionBy("year","month","region_id").orderBy(desc("total_amount"))))
      .select("province_id","province_name","region_id","region_name","total_amount","total_count","sequence","year","month")

    result1.show()

    result1.createOrReplaceTempView("province_consumption_day_aggr")
    spark.sql(
      """
        |select province_id,provnce_name,region_id,region_name,cast (total_amount as bigint),total_count,sequence,year,month
        |from dws.province_consumption_day_aggr
        |order by total_count desc,total_amount desc,province_id desc limit 5
        |""".stripMargin).show()
    // select province_id,province_name,region_id,region_name, cast(total_amount as bigint),total_count,sequence,year,month
    // from dws_ds_hudi.province_consumption_day_aggr order by total_count desc,total_amount desc,province_id desc limit 5;
//    result1.write.jdbc(url,"provinceeverymonth",prop)

  }
}
