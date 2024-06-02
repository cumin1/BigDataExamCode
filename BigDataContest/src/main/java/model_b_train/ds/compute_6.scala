package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/*
6、请根据dws_ds_hudi库中的表计算出每个省份2020年4月的平均订单金额和该省所在地区平均订单金额相比较结果（“高/低/相同”）,
存入ClickHouse数据库shtd_result的provinceavgcmpregion表中（表结构如下），
然后在Linux的ClickHouse命令行中根据省份表主键、省平均订单金额、地区平均订单金额均为降序排序，查询出前5条


字段	类型	中文含义	备注
provinceid	int	省份表主键
provincename	text	省份名称
provinceavgconsumption	double	该省平均订单金额
regionid	int	地区表主键
regionname	text	地区名称
regionavgconsumption	double	地区平均订单金额	该省所在地区平均订单金额
comparison	text	比较结果	省平均订单金额和该省所在地区平均订单金额比较结果，值为：高/低/相同
 */
object compute_6 {
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
        year(col("create_time")) as "year",
        month(col("create_time")) as "month"
      )
      .filter(col("year") === 2020 && col("month") === 4)
      .drop("year", "month")


    val frame1 = source.groupBy("regionid")
      .agg(
        avg("amount") as "regionavgconsumption"
      )

    val frame2 = source.groupBy("provinceid", "provincename", "regionid", "regionname")
      .agg(
        avg("amount") as "provinceavgconsumption"
      )

    val result = frame1.join(frame2, Seq("regionid"))
      .withColumn("comparison",
        when(col("provinceavgconsumption") > col("regionavgconsumption"), "高")
          .when(col("provinceavgconsumption") < col("regionavgconsumption"), "低")
          .otherwise("相同")
      )
      .select("provinceid", "provincename", "provinceavgconsumption", "regionid", "regionname", "regionavgconsumption", "comparison")

    result.createOrReplaceTempView("provinceavgcmpregion")
    spark.sql(
      """
        |select * from provinceavgcmpregion
        |order by provinceid desc,provinceavgconsumption desc,regionavgconsumption desc
        |limit 5
        |""".stripMargin).show()
  }
}
