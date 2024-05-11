package TaskBook2.wajue_toal


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

object feature1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书2 数据挖掘")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()
    import spark.implicits._

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    prop.put("driver","com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false"

    val order_detail = spark.read.jdbc(url, "order_detail", prop)
//    val sku_info = spark.read.jdbc(url,"sku_info",prop)
    val order_info = spark.read.jdbc(url,"order_info",prop)

    val data = order_detail.join(order_info, order_detail("order_id") === order_info("id"))
      .select("user_id", "sku_id")
      .distinct()

    data.show()
    val user6708_skuids: Array[Int] = data.filter(col("user_id") === 6708).select("sku_id").map(_(0).toString.toInt).collect()
    val user_ids: String = data
      .withColumn("cos", when(col("sku_id").isin(user6708_skuids: _*), 1.0).otherwise(0.0))
      .groupBy("user_id")
      .agg(sum("cos").as("same"))
      .filter(col("user_id") =!= 6708)
      .orderBy(desc("same"), asc("user_id"))
      .limit(10)
      .map(_(0).toString)
      .collect()
      .mkString(",")

    println(user_ids)

    spark.stop()
  }
}
/*
    val order_id_6708 = order_info
      .filter(col("user_id") === "6708")
      .select("id")
      .map(_(0).toString)
      .collect()

    val sku_id_6708 = order_detail  // 这个是6708所有买的商品id
      .filter(col("order_id").isin(order_id_6708: _*))
      .select("sku_id")
      .distinct()
      .map(_(0).toString)
      .collect()

    val order_id_list = order_detail
      .filter(col("sku_id").isin(sku_id_6708: _*))
      .select("order_id")
      .map(_(0).toString)
      .collect()

    val result = order_info
      .withColumn("count",
        when(col("id").isin(order_id_list: _*), 1).otherwise(0)
      )
      .groupBy("user_id")
      .agg(
        sum("count") as "counts"
      )
      .filter(col("user_id") =!= "6708")
      .orderBy(desc("counts"))
      .limit(10)
      .map(_(0).toString)
      .collect()
      .mkString(",")

    println(result)

 */

/*
val data = order_detail.join(order_info, order_detail("order_id") === order_info("id"))
      .select("user_id", "sku_id")
      .distinct()

    data.show()
    val user6708_skuids: Array[Int] = data.filter(col("user_id") === 6708).select("sku_id").map(_(0).toString.toInt).collect()
    val user_ids: String = data
      .withColumn("cos", when(col("sku_id").isin(user6708_skuids: _*), 1.0).otherwise(0.0))
      .groupBy("user_id")
      .agg(sum("cos").as("same"))
      .filter(col("user_id") =!= 6708)
      .orderBy(desc("same"), asc("user_id"))
      .limit(10)
      .map(_(0).toString)
      .collect()
      .mkString(",")

    println(user_ids)
 */