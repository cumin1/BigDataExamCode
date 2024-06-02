package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
2、请根据dwd层表计算出2020年4月每个省份的平均订单金额和所有省份平均订单金额相比较结果（“高/低/相同”）,
存入MySQL数据库shtd_result的provinceavgcmp表（表结构如下）中，
然后在Linux的MySQL命令行中根据省份表主键、该省平均订单金额均为降序排序，查询出前5条

字段	类型	中文含义	备注
provinceid	int	省份表主键
provincename	text	省份名称
provinceavgconsumption	double	该省平均订单金额
allprovinceavgconsumption	double	所有省平均订单金额
comparison	text	比较结果	该省平均订单金额和所有省平均订单金额比较结果，值为：高/低/相同
 */
object compute_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val province = spark.table("dwd.dim_province")
      .select("id","name")

    val order_info = spark.table("dwd.fact_order_info")
      .select("final_total_amount","create_time","province_id")

    val source = order_info.join(province, order_info("province_id") === province("id"))
      .withColumn("year", year(col("create_time")))
      .withColumn("month", month(col("create_time")))
      .filter(col("year") === 2020 && col("month") === 4)
      .withColumnRenamed("id", "provinceid")
      .withColumnRenamed("name", "provincename")
      .withColumnRenamed("final_total_amount", "amount")
      .select("provinceid", "provincename", "amount")

    val allprovinceavgconsumption = source
      .agg(avg("amount") as "allprovinceavgconsumption")
      .select("allprovinceavgconsumption").first().getDouble(0)

    val result = source.groupBy("provinceid", "provincename")
      .agg(
        avg("amount") as "provinceavgconsumption"
      )
      .withColumn("allprovinceavgconsumption", lit(allprovinceavgconsumption))
      .withColumn("comparison",
        when(col("provinceavgconsumption") > col("allprovinceavgconsumption"), "高")
          .when(col("provinceavgconsumption") < col("allprovinceavgconsumption"), "低")
          .otherwise("相同")
      )
      .select("provinceid", "provincename", "provinceavgconsumption", "allprovinceavgconsumption", "comparison")

    result.createOrReplaceTempView("provinceavgcmp")
    spark.sql(
      """
        |select * from provinceavgcmp
        |order by provinceid desc,provinceavgconsumption desc
        |limit 5
        |""".stripMargin).show()


  }
}
