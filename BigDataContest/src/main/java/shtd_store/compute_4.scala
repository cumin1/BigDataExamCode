package shtd_store



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, lit, sum}
import org.apache.spark.sql.types.DataTypes

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

    val fact_order_info = spark.table("dwd.fact_order_info")
    val dim_province = spark.table("dwd.dim_province").filter(col("etl_date") === "20240526")

    var source = fact_order_info.join(dim_province, fact_order_info("province_id") === dim_province("id"))
      .select(dim_province("name") as "province_name", fact_order_info("final_total_amount") as "amount")

    val arr = source
      .groupBy("province_name")
      .agg(count("amount") as "Amount")
      .withColumn("Amount",col("Amount").cast(DataTypes.LongType))
      .orderBy(desc("Amount"))
      .collect()

    println(arr.mkString(","))
//    val arr = spark.sql(
//      """
//        |select
//        |p.name,
//        |count(*)
//        |from fact_order_info o join (select * from dim_province where etl_date='20240526') p
//        |on p.id=o.province_id
//        |group by p.name
//        |""".stripMargin).collect()

//    var df = spark.sql("select 1 as a ").drop("a")

    for (data <- arr.sortBy(_.getLong(1)).reverse) {

      source = source.withColumn(data.getString(0), lit(data.getLong(1)))

    }

    source
      .drop("province_name","Amount")
      .limit(1)
      .show()

    spark.stop()
  }
}
