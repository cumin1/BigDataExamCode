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

    val fact_order_detail = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
      .select("order_id", "sku_id","sku_name","order_price","sku_num")
      .withColumn("sku_num",col("sku_num").cast("int"))

//    val fact_order_detail = spark.read.format("jdbc")
//      .option("url","jdbc:mysql://localhost:3306/test1?useSSL=false")
//      .option("user","root")
//      .option("password","123456")
//      .option("driver","com.mysql.jdbc.Driver")
//      .option("dbtable","order_detail")
//      .load()
//      .select("order_id", "sku_id","sku_name","order_price","sku_num")
//      .withColumn("sku_num",col("sku_num").cast("int"))


    // 先计算总销量
    val df1 = fact_order_detail.groupBy("sku_id", "sku_name")
      .agg(
        count("sku_num") as "topquantity"
      )
      .withColumn("tmp",lit("tmp"))
      .withColumn("sequence",row_number().over(Window.partitionBy("tmp").orderBy(desc("topquantity"))))
      .withColumnRenamed("sku_id","topquantityid")
      .withColumnRenamed("sku_name","topquantityname")
      .select("topquantityid","topquantityname","topquantity","sequence")
      .orderBy("sequence")
      .limit(10)

//    df1.show(5)

    // 再计算总销售额
    val df2 = fact_order_detail
      .withColumn("price", col("order_price") * col("sku_num"))
      .groupBy("sku_id", "sku_name")
      .agg(
        sum("price") as "topprice"
      )
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("topprice"))))
      .withColumnRenamed("sku_id", "toppriceid")
      .withColumnRenamed("sku_name", "toppricename")
      .select("toppriceid", "toppricename", "topprice", "sequence")
      .orderBy("sequence")
      .limit(10)

//    df2.show(5)

    val result = df1.join(df2, Seq("sequence"))
      .select("topquantityid", "topquantityname", "topquantity", "toppriceid", "toppricename", "topprice", "sequence")

    result.createOrReplaceTempView("result")
    spark.sql("select * from result order by sequence asc limit 5").show()


//    val properties = new Properties() {
//      {
//        setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
//        setProperty("user", "default")
//        setProperty("password", "123456")
//      }
//    }

//    result.write.mode("append").jdbc("jdbc:clickhouse://bigdata1:8123/shtd_result?useSSL=false","topten",properties)
    // select topquantityid, topquantity,toppriceid, topprice,sequence  from topten order by sequence asc limit 5;
    // create table topten (topquantityid int,topquantityname String,topquantity int,toppriceid String,toppricename String,topprice decimal(65,2),sequence int) engine=Memory;

    spark.stop()
  }
}
