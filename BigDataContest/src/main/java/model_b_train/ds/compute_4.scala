package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.util.UUID

/*
4、根据dwd层表统计每人每天下单的数量和下单的总金额，
存入Hudi的dws_ds_hudi层的user_consumption_day_aggr表中（表结构如下），
然后使用spark -shell按照客户主键、订单总金额均为降序排序，查询出前5条

字段	类型	中文含义	备注
uuid	string	随机字符	随机字符，保证不同即可，作为primaryKey
user_id	int	客户主键
user_name	string	客户名称
total_amount	double	订单总金额	当天订单总金额。
total_count	int	订单总数	当天订单总数。同时可作为preCombineField（作为合并字段时，无意义，因为主键为随机生成）
year	int	年	订单产生的年,为动态分区字段
month	int	月	订单产生的月,为动态分区字段
day	int	日	订单产生的日,为动态分区字段
 */
object compute_4 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val user = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
      .filter(col("etl_date") === "20240521")
      .select("id","name")

    val order = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .select("user_id","final_total_amount","create_time")

    val uuid = spark.udf.register("uuid", () => {
      UUID.randomUUID().toString
    })

    val result = order.join(user, order("user_id") === user("id"))
      .select(
        user("id") as "user_id",
        user("name") as "user_name",
        order("final_total_amount") as "amount",
        year(col("create_time")) as "year",
        month(col("create_time")) as "month",
        dayofmonth(col("create_time")) as "day"
      )
      .groupBy("user_id","user_name","year","month","day")
      .agg(
        sum("amount") as "total_amount",
        count("amount") as "total_count"
      )
      .withColumn("uuid",uuid())
      .select("uuid","user_id","user_name","total_amount","total_count","year","month","day")
      .withColumn("total_amount",col("total_amount").cast("double"))

    result.createOrReplaceTempView("user_consumption_day_aggr")
    spark.sql(
      """
        |select * from user_consumption_day_aggr
        |order by user_id desc,total_amount desc
        |limit 5
        |""".stripMargin).show()

  }
}
