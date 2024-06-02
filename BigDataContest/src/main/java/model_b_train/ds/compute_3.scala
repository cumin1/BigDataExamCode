package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/*
3、根据dwd层表统计在两天内连续下单并且下单金额保持增长的用户，
存入MySQL数据库shtd_result的usercontinueorder表(表结构如下)中，
然后在Linux的MySQL命令行中根据订单总数、订单总金额、客户主键均为降序排序，查询出前5条

字段	类型	中文含义	备注
userid	int	客户主键
username	text	客户名称
day	text	日	记录下单日的时间，格式为yyyyMMdd_yyyyMMdd 例如： 20220101_20220102
totalconsumption	double	订单总金额	连续两天的订单总金额
totalorder	int	订单总数	连续两天的订单总数
 */
object compute_3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val user = spark.table("dwd.dim_user_info")
      .select("id","name")
    val order = spark.table("dwd.fact_order_info")
      .select("user_id","final_total_amount","create_time")

    val source = order.join(user, order("user_id") === user("id"))
      .select(
        user("id") as "userid",
        user("name") as "username",
        order("final_total_amount") as "amount",
        to_timestamp(col("create_time"), "yyyy-MM-dd") as "tmp_day"
      )
      .groupBy("userid", "username", "tmp_day")
      .agg(
        sum("amount") as "totalconsumption",
        count("amount") as "totalorder"
      )

    val result = source.as("df1")
      .join(source.as("df2"), col("df1.userid") === col("df2.userid") and datediff(col("df1.tmp_day"), col("df2.tmp_day")) === 1)
      .withColumn("day", concat(date_format(col("df1.tmp_day"),"yyyyMMdd"), lit("_"), date_format(col("df2.tmp_day"),"yyyyMMdd")))
      .filter(col("df1.totalconsumption") < col("df2.totalconsumption"))
      .select(
        col("df1.userid") as "userid",
        col("df1.username") as "username",
        col("day"),
        (col("df1.totalconsumption") + col("df2.totalconsumption")) as "totalconsumption",
        (col("df1.totalorder") + col("df2.totalorder")) as "totalorder"
      )

    result.createOrReplaceTempView("usercontinueorder")
    spark.sql(
      """
        |select * from usercontinueorder
        |order by totalorder desc,totalconsumption desc,userid desc
        |limit 5
        |""".stripMargin).show()

  }
}
