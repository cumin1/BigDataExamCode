package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/*
8、根据dws层表来计算每个地区2020年订单金额前3省份，
依次存入MySQL数据库shtd_result的regiontopthree表中（表结构如下），
然后在Linux的MySQL命令行中根据地区表主键升序排序，查询出前5条

字段	类型	中文含义	备注
regionid	int	地区表主键
regionname	text	地区名称
provinceids	text	省份表主键	用,分割显示前三省份的id
provincenames	text	省份名称	用,分割显示前三省份的name
provinceamount	text	省份名称	用,分割显示前三省份的订单金额（需要去除小数部分，使用四舍五入）
例如：
3	华东地区	21,27,11	上海市,江苏省,浙江省	100000,100,10

 */
object compute_9 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val data = spark.table("dws.province_consumption_day_aggr")
//    data.show()

    val source = data.filter(col("year") === 2020)
      .drop("total_count", "year", "month")

    val result = source
      .filter(col("sequence") <= 3)
      .withColumn("province_id_2", lead("province_id", 1).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("province_id_3", lead("province_id", 2).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("province_name_2", lead("provnce_name", 1).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("province_name_3", lead("provnce_name", 2).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("total_amount_2", lead("total_amount", 1).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("total_amount_3", lead("total_amount", 2).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .filter(col("sequence") === 1)
      .withColumn("provinceids", concat(col("province_id"), lit(","), col("province_id_2"), lit(","), col("province_id_3")))
      .withColumn("provincenames", concat(col("provnce_name"), lit(","), col("province_name_2"), lit(","), col("province_name_3")))
      .withColumn("provinceamount", concat(col("total_amount"), lit(","), col("total_amount_2"), lit(","), col("total_amount_3")))
      .withColumnRenamed("region_id", "regionid")
      .withColumnRenamed("region_name", "regionname")
      .select("regionid", "regionname", "provinceids", "provincenames", "provinceamount")

    result.createOrReplaceTempView("regiontopthree")
    spark.sql(
      """
        |select * from regiontopthree
        |order by regionid asc
        |limit 5
        |""".stripMargin).show()

  }
}
