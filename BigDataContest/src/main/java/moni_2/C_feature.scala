package moni_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes

import java.util.Properties

object C_feature {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("特征工程")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()
    val order_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info")
      .load().select("id","user_id")

    val order_detail = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_detail")
      .load().select("order_id","sku_id")

    import spark.implicits._
    val source = order_info.join(order_detail, order_info("id") === order_detail("order_id"))
      .select("user_id", "sku_id")
      .distinct()

    val res1 = source
      .withColumn("user_id", dense_rank().over(Window.orderBy("user_id")) - 1)
      .withColumn("sku_id", dense_rank().over(Window.orderBy("sku_id")) - 1)
      .orderBy(asc("user_id"), asc("sku_id"))

    import spark.implicits._

    println("-------user_id_mapping与sku_id_mapping数据前5条如下：-------")
    res1
      .limit(5)
      .foreach(r => {
        println(r(0) + ":" + r(1))
      })

    val source1 = res1
      .withColumn("sku_id", concat(lit("sku_id"), col("sku_id")))

    val sku_ids = source1.select("sku_id").distinct()
      .orderBy(split(col("sku_id"),"id")(1).cast("int"))
      .map(_(0).toString).collect()

    val res2 = source1
      .groupBy("user_id")
      .pivot("sku_id", sku_ids)
      .agg(
        lit(1.0)
      )
      .na
      .fill(0.0)
      .withColumn("user_id", col("user_id").cast("double"))
      .orderBy("user_id")

    res2.show(5)

    println("---------------第一行前5列结果展示为---------------")
    println(res2.limit(1).select("user_id","sku_id0","sku_id1","sku_id2","sku_id3").collect().mkString(",").replace("[","").replace("]",""))

  }
}
