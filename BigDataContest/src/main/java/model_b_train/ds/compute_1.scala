package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/*
1、根据dwd层表统计每个省份、每个地区、每个月下单的数量和下单的总金额，
存入MySQL数据库shtd_result的provinceeverymonth表中（表结构如下），
然后在Linux的MySQL命令行中根据订单总数、订单总金额、省份表主键均为降序排序，查询出前5条

字段	类型	中文含义	备注
provinceid	int	省份表主键
provincename	text	省份名称
regionid	int	地区表主键
regionname	text	地区名称
totalconsumption	double	订单总金额	当月订单总金额
totalorder	int	订单总数	当月订单总数
year	int	年	订单产生的年
month	int	月	订单产生的月
 */
object compute_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val province = spark.table("dwd.dim_province")
      .select("id","name","region_id")
    val region = spark.table("dwd.dim_region")
      .select("id","region_name")
    val order_info = spark.table("dwd.fact_order_info")
      .select("final_total_amount","create_time","province_id")

    val result = order_info.join(province, order_info("province_id") === province("id"))
      .join(region, province("region_id") === region("id"))
      .select(
        province("id") as "provinceid",
        province("name") as "provincename",
        region("id") as "regionid",
        region("region_name") as "regionname",
        order_info("final_total_amount") as "amount",
        year(order_info("create_time")) as "year",
        month(order_info("create_time")) as "month",
      )
      .groupBy("provinceid", "provincename", "regionid", "regionname", "year", "month")
      .agg(
        sum("amount") as "totalconsumption",
        count("amount") as "totalorder"
      )
      .select("provinceid", "provincename", "regionid", "regionname", "totalconsumption", "totalorder", "year", "month")

    result.createOrReplaceTempView("provinceeverymonth")
    spark.sql(
      """
        |select * from provinceeverymonth
        |order by totalorder desc,totalconsumption desc,provinceid desc
        |limit 5
        |""".stripMargin).show()

    spark.stop()
  }
}
