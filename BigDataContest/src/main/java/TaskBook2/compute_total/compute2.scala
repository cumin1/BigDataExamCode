package TaskBook2.compute_total

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, desc, from_unixtime, lit, month, row_number, sum, unix_timestamp, year}

import java.util.UUID

object compute2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书2 计算2")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    // todo 指标计算第二题 每个省每月下单的数量和下单的总金额
    /*
    字段	类型	中文含义	备注
     uuid	string	随机字符	随机字符，保证不同即可，作为primaryKey
    province_id	int	省份表主键
    province_name	string	省份名称
    region_id	int	地区主键
    region_name	string	地区名称
    total_amount	double	订单总金额	当月订单总金额
    total_count	int	订单总数	当月订单总数。同时可作为preCombineField（作为合并字段时，无意义，因为主键为随机生成）
    sequence	int	次序
    year	int	年	订单产生的年,为动态分区字段
    month	int	月	订单产生的月,为动态分区字段

     */
    val uuidfuc = spark.udf.register("uuid",()=>{
      var uuid = UUID.randomUUID().toString.replace("-", "")
      uuid
    })

    val order_info = spark.table("dwd_ds_hudi.fact_order_info")
      .select("province_id","final_total_amount","create_time")
    val dim_province = spark.table("dwd_ds_hudi.dim_province")
      .select("id","name","region_id")
    val dim_region = spark.table("dwd_ds_hudi.dim_region")
      .select("id","region_name")

//    order_info.show(1)
//    dim_province.show(1)
//    dim_region.show(1)

    val source1 = order_info
      .join(dim_province,order_info("province_id") === dim_province("id"))
      .join(dim_region,dim_province("region_id") === dim_region("id"))
      .select(
        col("province_id") as "province_id",
        dim_province("name") as "province_name",
        dim_region("id") as "region_id",
        dim_region("region_name") as "region_name",
        order_info("final_total_amount") as "amount",
        year(from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"),"yyyy-MM-dd HH:mm:ss")) as "year",
        month(from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"),"yyyy-MM-dd HH:mm:ss")) as "month",
      )

    val result = source1
      .groupBy(
        "province_id", "province_name", "region_id", "region_name", "year", "month"
      )
      .agg(
        sum("amount") as "total_amount",
        count("amount") as "total_count"
      )
      .withColumn("sequence", row_number().over(Window.partitionBy("year","month","region_id").orderBy(desc("total_amount"))))
      .withColumn("uuid", lit(uuidfuc()))
      .select(
        "uuid", "province_id", "province_name", "region_id", "region_name", "total_amount", "total_count", "sequence", "year", "month"
      )

    result.show(5)
    result.createTempView("province_consumption_day_aggr")
    spark.sql(
      "select * from province_consumption_day_aggr order by total_count desc,total_amount desc,province_id desc limit 5"
    ).show()


    spark.stop()
  }
}
