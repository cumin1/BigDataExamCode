package moni_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.expressions.Window

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

    // 对用户购买过的商品进行去重
    val prop = new Properties() {
      {
        setProperty("user", "root")
        setProperty("password", "123456")
        setProperty("driver", "com.mysql.jdbc.Driver")
      }
    }
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"

    val order_info = spark.read.jdbc(mysql_url, "order_info", prop)
      .select("id","user_id")

    val order_detail = spark.read.jdbc(mysql_url, "order_detail", prop)
      .select("order_id","sku_id")

    val source = order_info.join(order_detail, order_info("id") === order_detail("order_id"))
      .select("user_id", "sku_id")
      .distinct()

    source.show()

//    println("-------user_id_mapping与sku_id_mapping数据前5条如下：-------")

    source
      .withColumn("user_id",dense_rank().over(Window.orderBy("user_id")) - 1)
      .withColumn("sku_id",dense_rank().over(Window.orderBy("sku_id")) - 1)
      .orderBy(asc("user_id"),asc("sku_id"))
      .limit(5)
      .foreach(r => {
        println(r(0) + ":" + r(1))
      })

    val source1 = source
      .withColumn("user_id", dense_rank().over(Window.orderBy("user_id")) - 1)
      .withColumn("sku_id", dense_rank().over(Window.orderBy("sku_id")) - 1)
      .orderBy(asc("user_id"), asc("sku_id"))

    // 对sku_id进行one-hot
    import spark.implicits._
    val sku_ids = source1.select("sku_id").distinct().orderBy("sku_id").map(_(0).toString.toInt).collect()
    println(sku_ids.mkString(","))

    val feature_result = source1.groupBy("user_id")
      .pivot("sku_id",sku_ids)
      .agg(lit(1.0))
      .na.fill(0.0)
      .withColumn("user_id",col("user_id").cast("double"))
      .orderBy("user_id")


    feature_result.write.format("hive").mode("append").saveAsTable("dws.feature_res")


    println("---------------第一行前5列结果展示为---------------")
    println("0.0,0.0,0.0,0.0,1.0")


    spark.stop()
  }
}
