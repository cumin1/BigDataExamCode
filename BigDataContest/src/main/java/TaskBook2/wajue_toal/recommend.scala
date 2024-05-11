package TaskBook2.wajue_toal


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{LabeledPoint, Normalizer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.linalg.{SparseVector, Vector}

import java.util.Properties

object recommend {
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

    val order_detail = spark.read.jdbc("jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false", "order_detail", prop)

    val order_info = spark.read.jdbc("jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false", "order_info", prop)

    val user_buy_sku = order_detail
      .as("order_detail")
      .join(order_info.as("order_info"), expr("order_info.id = order_detail.order_id"))
      .select("user_id", "sku_id")
      // 题目要求不考虑同一个商品多次购买的情况所以需要去重
      .distinct()

    import spark.implicits._

    val user_6708_sku_ids: Array[Double] = user_buy_sku
      .filter(col("user_id") === 6708)
      .select("sku_id")
      .map(_ (0).toString.toDouble)
      .collect()

    val other_10_user_ids = user_buy_sku
      .filter(col("user_id") !== 6708)
      .withColumn("is_cos", when(col("sku_id").cast(DoubleType).isin(user_6708_sku_ids:_*), 1).otherwise(0))
      .groupBy(col("user_id"))
      .agg(sum("is_cos").as("count_cos"))
      .orderBy(col("count_cos").desc)
      .select("user_id")
      .map(_ (0).toString.toLong)
      .limit(10)
      .collect()

    val other_10_sku_ids = user_buy_sku.filter(col("user_id").isin(other_10_user_ids:_*))
      .select("sku_id")
      .map(_ (0).toString.toDouble)
      .collect()

    val sku_info_vector = spark.table("dwd.sku_info_vector")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(sku_info_vector.columns.slice(1, sku_info_vector.columns.length))
      .setOutputCol("features")
    val dataFrame = new Pipeline()
      .setStages(Array(vectorAssembler))
      .fit(sku_info_vector)
      .transform(sku_info_vector)

    val mapData = dataFrame
      .select("id", "features")
      .map(r => {
        LabeledPoint(r(0).toString.toDouble, r(1).asInstanceOf[Vector])
      })
    val normalizer = new Normalizer().setInputCol("features").setOutputCol("norm_features").setP(2.0)
    val normalized_data = normalizer.transform(mapData).select("label", "norm_features")

    spark.udf.register("cos", (v1: SparseVector, v2: SparseVector) => {
      1 - breeze.linalg.functions.cosineDistance(breeze.linalg.DenseVector(v1.values), breeze.linalg.DenseVector(v2.values))
    })

    normalized_data.crossJoin(normalized_data)
      .toDF("left_label", "left_norm_vector", "right_label", "right_norm_vector")
      .filter(col("left_label") !== col("right_label"))
      .withColumn("cos", expr("cos(left_norm_vector,right_norm_vector)"))
      .orderBy(desc("cos"))
      // left_label 用户购买的   right_label 其他用户购买的并剔除指定用户购买
      .filter(col("left_label").isin(user_6708_sku_ids:_*))
      .filter(!col("right_label").isin(user_6708_sku_ids:_*) && col("right_label").isin(other_10_sku_ids:_*))
      .groupBy("right_label")
      .agg(avg("cos").as("cos"))
      .orderBy(desc("cos"))
      .limit(5)
      .show(false)


  }
}
