package TaskBook6


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import java.util.Properties

object C_feature {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder().appName("任务书6 特征工程")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val properties: Properties = new Properties() {
      {
        setProperty("driver", "com.mysql.jdbc.Driver")
        setProperty("user", "root")
        setProperty("password", "123456")
      }
    }
    val mysql_url: String = "jdbc:mysql://219.228.173.114:3306/shtd_store?useSSL=false"

    val order_detail = spark.read.jdbc(mysql_url, "order_detail", properties).select("order_id","sku_id")
    val order_info = spark.read.jdbc(mysql_url, "order_info", properties).select("id", "user_id")

    val source = order_info.join(order_detail, order_info("id") === order_detail("order_id"))
      .dropDuplicates("user_id", "sku_id")
      .select("user_id", "sku_id")
      .withColumn("user_id", dense_rank().over(Window.orderBy("user_id")) - 1)
      .withColumn("sku_id", dense_rank().over(Window.orderBy("sku_id")) - 1)
      .orderBy("user_id","sku_id")

    import spark.implicits._
    source
      .limit(5)
      .foreach(r => {
        println(r(0) + ":" + r(1))
      })

    val sku_ids: Array[String] = source.select("sku_id").distinct().orderBy(split(col("sku_id"), "id")(1).cast("int"))
      .map(r =>{
        "sku_id" + r(0).toString
      }).collect()

    println(sku_ids.mkString("Array(", ", ", ")"))

    source.groupBy("user_id")
      .pivot("sku_id", sku_ids)
      .agg(lit(1.0))
      .na
      .fill(0.0)
      .withColumn("user_id", col("user_id").cast(DoubleType))
      .orderBy("user_id")
      .show()




    spark.stop()
  }
}
