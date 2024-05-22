package moni_1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.expressions.Window

import java.util
import java.util.{Properties, UUID}
object B_compute_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    // 计算2020年销售量前10的商品
    val fact_order_info = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .withColumn("year",year(col("create_time")))
      .select("id","final_total_amount","year")

    val fact_order_detail = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
      .select("order_id", "sku_id","sku_name","sku_num")

//    fact_order_info.show(5)
//    fact_order_detail.show(5)

    val source = fact_order_info
      .join(fact_order_detail, fact_order_info("id") === fact_order_detail("order_id"))
      .select("id", "final_total_amount", "year", "order_id", "sku_id", "sku_name","sku_num")
      .filter(col("year") === 2020)
      .groupBy("sku_id","sku_name")
      .agg(
        sum("sku_num") as "sku_num",
        sum("final_total_amount") as "final_total_amount"
      )

    val frame1 = source
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("sku_num"))))
      .drop("tmp")
      .filter(col("sequence") <= 10)
      .withColumnRenamed("sku_id", "topquantityid")
      .withColumnRenamed("sku_name", "topquantityname")
      .withColumnRenamed("sku_num","topquantity")
      .select(
        "topquantityid", "topquantityname", "topquantity", "sequence"
      )


    val frame2 = source
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("final_total_amount"))))
      .drop("tmp")
      .filter(col("sequence") <= 10)
      .withColumnRenamed("sku_id", "toppriceid")
      .withColumnRenamed("sku_name", "toppricename")
      .withColumnRenamed("final_total_amount", "topprice")
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

    val properties = new Properties() {
      {
        setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        setProperty("user", "default")
        setProperty("password", "123456")
      }
    }

    result.write.mode("append").jdbc("jdbc:clickhouse://bigdata1:8123/shtd_result?useSSL=false","topten",properties)
    // select topquantityid, topquantity,toppriceid, topprice,sequence  from topten order by sequence asc limit 5;
    // create table topten (topquantityid int,topquantityname String,topquantity int,toppriceid String,toppricename String,topprice decimal(65,2),sequence int) engine=Memory;

    spark.stop()
  }
}
