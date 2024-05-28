package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object compute_4 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("sjr")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

//    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("use dwd")

    val arr = spark.sql(
      """
        |select
        |p.name,
        |count(*)
        |from fact_order_info o join (select * from dim_province where etl_date='20240526') p
        |on p.id=o.province_id
        |group by p.name
        |""".stripMargin).collect()

    var df = spark.sql("select 1 as a ").drop("a")

    for (data <- arr.sortBy(_.getLong(1)).reverse) {

      df = df.withColumn(data.getString(0), lit(data.getLong(1)))

    }
    df.show

    spark.stop()
  }
}
