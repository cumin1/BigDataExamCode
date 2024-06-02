package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_7 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("前10")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val fact_order_detail = spark.read.format("hudi")
      .load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
      .select("sku_id","sku_name","order_id","sku_num","order_price")

    fact_order_detail.show(5)

    val source = fact_order_detail
      .select(
        fact_order_detail("order_price"),
        fact_order_detail("sku_id"),
        fact_order_detail("sku_name"),
        fact_order_detail("sku_num")
      )

    val df1 = source
      .groupBy("sku_id", "sku_name")
      .agg(
        count("order_price") as "topquantity"
      )
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("topquantity"))))
      .drop("tmp")
      .filter(col("sequence") <= 10)
      .withColumnRenamed("sku_id", "topquantityid")
      .withColumnRenamed("sku_name", "topquantityname")

    val df2 = source
      .withColumn("price", col("order_price") * col("sku_num"))
      .groupBy("sku_id", "sku_name")
      .agg(
        sum("price") as "topprice"
      )
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("topprice"))))
      .drop("tmp")
      .filter(col("sequence") <= 10)
      .withColumnRenamed("sku_id", "toppriceid")
      .withColumnRenamed("sku_name", "toppricename")

    val res = df1.join(df2, Seq("sequence"))
      .withColumn("topprice", col("topprice").cast("bigint"))
      .select("topquantityid", "topquantityname", "topquantity", "toppriceid", "toppricename", "topprice", "sequence")

    res.show()

    res.createOrReplaceTempView("result")
    spark.sql("select topquantityid,topquantity,toppriceid,topprice,sequence from result order by sequence asc limit 5").show()

    spark.stop()
  }
}
