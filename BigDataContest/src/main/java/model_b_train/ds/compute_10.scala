package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.util.UUID

/*
9、根据dwd_ds_hudi层表统计每个省每月下单的数量和下单的总金额，
并按照year，month，region_id进行分组,按照total_amount降序排序，形成sequence值，
将计算结果存入Hudi的dws_ds_hudi数据库province_consumption_day_aggr表中（表结构如下），
然后使用spark-shell根据订单总数、订单总金额、省份表主键均为降序排序，查询出前5条

字段	类型	中文含义	备注
uuid	string	随机字符	随机字符，保证不同即可，作为primaryKey
province_id	int	省份主键
province_name	string	省份名称
region_id	int	地区主键
region_name	string	地区名称
total_amount	double	订单总金额	当月订单总金额
total_count	int	订单总数	当月订单总数。同时可作为preCombineField（作为合并字段时，无意义，因为主键为随机生成）
sequence	int	次序
year	int	年	订单产生的年,为动态分区字段
month	int	月	订单产生的月,为动态分区字段
 */
object compute_10 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val province = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_province")
      .select("region_id","id","name")
    val region = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_region")
      .select("region_name","id")
    val order = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .select("province_id","final_total_amount","create_time")

    val uuid = spark.udf.register("uuid", () => {
      UUID.randomUUID().toString
    })

    val result = order.join(province, order("province_id") === province("id"))
      .join(region, province("region_id") === region("id"))
      .select(
        province("id") as "province_id",
        province("name") as "province_name",
        region("id") as "region_id",
        region("region_name") as "region_name",
        order("final_total_amount") as "amount",
        year(col("create_time")) as "year",
        month(col("create_time")) as "month"
      )
      .groupBy("province_id", "province_name", "region_id", "region_name", "year", "month")
      .agg(
        sum("amount") as "total_amount",
        count("amount") as "total_count"
      )
      .withColumn("uuid", uuid())
      .withColumn("sequence", row_number().over(Window.partitionBy("year", "month", "region_id").orderBy(desc("total_amount"))))
      .select("uuid", "province_id", "province_name", "region_id", "region_name", "total_amount", "total_count", "sequence", "year", "month")

    result.createOrReplaceTempView("province_consumption_day_aggr")
    spark.sql(
      """
        |select province_id,province_name,region_id,region_name,cast(total_amount as bigint),total_count,sequence,year,month
        |from province_consumption_day_aggr
        |order by total_count desc,total_amount desc,province_id desc
        |limit 5
        |""".stripMargin).show()

  }
}
