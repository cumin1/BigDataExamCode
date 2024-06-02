package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/*
11、请根据dwd_ds_hudi层的相关表，计算出2020年每个省份所在地区的订单金额的中位数,
存入ClickHouse数据库shtd_result的nationmedian表中（表结构如下），
然后在Linux的ClickHouse命令行中根据地区表主键，省份表主键均为升序排序，查询出前5条

提示：可用percentile函数求取中位数。
字段	类型	中文含义	备注
provinceid	int	省份表主键
provincename	text	省份名称
regionid	int	地区表主键
regionname	text	地区名称
provincemedian	double	该省份中位数	该省份订单金额中位数
regionmedian	double	该省所在地区中位数	该省所在地区订单金额中位数
 */
object compute_12 {
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

    val source = order.join(province, order("province_id") === province("id"))
      .join(region, province("region_id") === region("id"))
      .select(
        province("id") as "provinceid",
        province("name") as "provincename",
        region("id") as "regionid",
        region("region_name") as "regionname",
        order("final_total_amount") as "amount",
        year(col("create_time")) as "year"
      )
      .filter(col("year") === 2020)
      .drop("year")

    val frame1 = source
      .groupBy("provinceid", "provincename", "regionid", "regionname")
      .agg(
        expr("percentile(amount,0.5)") as "provincemedian"
      )

    val frame2 = source
      .groupBy("regionid")
      .agg(
        expr("percentile(amount,0.5)") as "regionmedian"
      )

    val result = frame1.join(frame2, Seq("regionid"))
      .select("provinceid", "provincename", "regionid", "regionname", "provincemedian", "regionmedian")

    result.createOrReplaceTempView("nationmedian")
    spark.sql(
      """
        |select * from nationmedian
        |order by regionid asc,provinceid asc
        |limit 5
        |""".stripMargin).show()
  }
}
