package Mode_c

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
object store_2_feature {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("电商2 特征工程")
      .master("local[*]")
      .enableHiveSupport()
      .config("hive.metastore.uris","thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    import spark.implicits._

    // todo 1

    val order_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info")
      .load()

    val order_detail = spark.read.format("jdbc")
      .option("url","jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","order_detail")
      .load()

    val source = order_info
      .join(order_detail, order_info("id") === order_detail("order_id"))
      .dropDuplicates("user_id","sku_id")
      .select("user_id", "sku_id")


    val feature1_res = source.orderBy(asc("user_id"), asc("sku_id"))
      .withColumn("user_id", dense_rank().over(Window.orderBy("user_id")) - 1)
      .withColumn("sku_id", dense_rank().over(Window.orderBy("sku_id")) - 1)
      .orderBy("user_id", "sku_id")

    println("-------------------相同种类前10的id结果展示为：--------------------")
    feature1_res
      .limit(5)
      .foreach{ r => {
        println(r(0) + ":" + r(1))
      }}

    // todo 2
    val data = feature1_res
      .withColumn("sku_id", concat(lit("sku_id"), col("sku_id")))
    data.show(5)
    // 把所有sku_id拿出来 做一下去重
    val sku_ids = data.select("sku_id").distinct()
      .orderBy(split(col("sku_id"),"id")(1).cast("int"))
      .map(_(0).toString).collect()

    val feature2_res = data.groupBy("user_id")
      .pivot("sku_id",sku_ids)
      .agg(lit(1.0))
      .na
      .fill(0.0)
      .withColumn("user_id",col("user_id").cast(DoubleType))
      .orderBy("user_id")

    feature2_res.show(5)

    spark.sql("create database dws")
    feature2_res.write.format("hive").mode("overwrite").saveAsTable("dws.store_2_feature_res")

    println("---------------第一行前5列结果展示为---------------")
    feature2_res.orderBy(asc("user_id")).limit(1)
      .select("user_id", "sku_id0", "sku_id1", "sku_id2", "sku_id3")
      .show()


    spark.stop()
  }
}
