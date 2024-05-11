package TaskBook6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object B_compute2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("任务书6 计算")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val fact_order_info = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .withColumn("year",year(from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"))))
      .select("id","final_total_amount","year")

    val fact_order_detail = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
      .select("order_id","sku_id","sku_name")

    val source = fact_order_info
      .join(fact_order_detail, fact_order_info("id") === fact_order_detail("order_id"))
      .select("id", "final_total_amount", "year", "order_id", "sku_id", "sku_name")
      .filter(col("year") === 2020)

    val frame1 = source.groupBy("sku_id", "sku_name")
      .agg(
        count("final_total_amount") as "topquantity"
      )
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("topquantity"))))
      .drop("tmp")
      .filter(col("sequence") <= 10)
      .withColumnRenamed("sku_id", "topquantityid")
      .withColumnRenamed("sku_name", "topquantityname")
      .select(
        "topquantityid", "topquantityname", "topquantity", "sequence"
      )


    val frame2 = source.groupBy("sku_id", "sku_name")
      .agg(
        sum("final_total_amount") as "topprice"
      )
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("topprice"))))
      .drop("tmp")
      .filter(col("sequence") <= 10)
      .withColumnRenamed("sku_id", "toppriceid")
      .withColumnRenamed("sku_name", "toppricename")
      .select(
        "toppriceid", "toppricename", "topprice", "sequence"
      )

    val result = frame1.join(frame2, Seq("sequence"))
      .select(
        col("topquantityid") as "topquantityid",
        col("topquantityname") as "topquantityname",
        col("topquantity") as "topquantity",
        col("toppriceid") as "toppriceid",
        col("toppricename") as "toppricename",
        col("topprice") as "topprice",
        frame1("sequence") as "sequence"
      )

    result.show()
    result.createTempView("topten")
    spark.sql("select * from topten order by sequence asc limit 5").show()




    spark.stop()
  }
}
