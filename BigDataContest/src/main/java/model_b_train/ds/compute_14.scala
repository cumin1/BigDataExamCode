package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/*
13、根据dwd层的数据，请计算连续两天下单的用户与已下单用户的占比，
将结果存入MySQL数据库shtd_result的userrepurchasedrate表中(表结构如下)，
然后在Linux的MySQL命令行中查询结果数据，

字段	类型	中文含义	备注
purchaseduser	int	下单人数	已下单人数
repurchaseduser	int	连续下单人数	连续两天下单的人数
repurchaserate	text	百占比	连续两天下单人数/已下单人数百分比（保留1位小数，四舍五入，不足的补0）例如21.1%，或者32.0%
 */
object compute_14 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val order = spark.table("dwd.fact_order_info")

    val purchaseduser = order.select("user_id").distinct().count()
//    println("已下单人数为：",purchaseduser)


    val source = order
      .select("user_id", "create_time")
      .withColumn("day", to_timestamp(col("create_time"), "yyyy-MM-dd"))

    val repurchaseduser = source.as("df1")
      .join(source.as("df2"), col("df1.user_id") === col("df2.user_id") && datediff(col("df1.day"), col("df2.day")) === 1)
      .select(
        col("df2.user_id") as "uid"
      )
      .select("uid")
      .distinct().count()

//    println("连续两天下单的人数：",repurchaseduser)

    val result = spark.createDataFrame(Seq(
      (purchaseduser, repurchaseduser)
    )).toDF("purchaseduser", "repurchaseduser")
      .withColumn("repurchaserate",concat(round(col("repurchaseduser") / col("purchaseduser") , 1),lit("%")))

    result.createOrReplaceTempView("userrepurchasedrate")
    spark.sql("select * from userrepurchasedrate").show()

  }
}
