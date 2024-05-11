package TaskBook2.compute_total

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, from_unixtime, month, unix_timestamp, when, year}

object compute3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书2 计算3")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    // todo 计算出每个省份2020年4月的平均订单金额和该省所在地区平均订单金额相比较结果
    /*
    字段	类型	中文含义	备注
    provinceid	int	省份表主键
    provincename	text	省份名称
    provinceavgconsumption	double	该省平均订单金额
    region_id	int	地区表主键
    region_name	text	地区名称
    regionavgconsumption	double	地区平均订单金额	该省所在地区平均订单金额
    comparison	text	比较结果	省平均订单金额和该省所在地区平均订单金额比较结果，值为：高/低/相同
     */

    val order_info = spark.table("dwd_ds_hudi.fact_order_info")
      .select("province_id","final_total_amount","create_time")
    val dim_province = spark.table("dwd_ds_hudi.dim_province")
      .select("id","name","region_id")
    val dim_region = spark.table("dwd_ds_hudi.dim_region")
      .select("id","region_name")


    val source1 = order_info
      .join(dim_province,order_info("province_id") === dim_province("id"))
      .join(dim_region,dim_province("region_id") === dim_region("id"))
      .select(
        col("province_id") as "provinceid",
        dim_province("name") as "provincename",
        dim_region("id") as "regionid",
        dim_region("region_name") as "regionname",
        order_info("final_total_amount") as "amount",
        year(from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"),"yyyy-MM-dd HH:mm:ss")) as "year",
        month(from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"),"yyyy-MM-dd HH:mm:ss")) as "month",
      )
      .filter(col("year") === "2020")
      .filter(col("month") === "4")


    val source2 = source1
      .groupBy(
        "provinceid", "provincename", "regionid", "regionname"
      )
      .agg(
        avg("amount") as "provinceavgconsumption"
      )

    val source3 = source1
      .groupBy(
        "regionid"
      )
      .agg(
        avg("amount") as "regionavgconsumption"
      )
      .withColumnRenamed("regionid", "rid")

    val result = source2
      .join(source3,source2("regionid") === source3("rid"))
      .withColumn("comparison",
        when(col("provinceavgconsumption") > col("regionavgconsumption"),"高")
        .when(col("provinceavgconsumption") < col("regionavgconsumption"),"低")
          .otherwise("相同")
      )
      .select("provinceid", "provincename","provinceavgconsumption" ,"regionid", "regionname","regionavgconsumption","comparison")

    result.show(5)
    result.createTempView("provinceavgcmpregion")
    spark.sql("select * from provinceavgcmpregion order by provinceid desc,provinceavgconsumption desc,regionavgconsumption desc limit 5").show()

    spark.stop()
  }
}
