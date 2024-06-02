package Mode_c.Moxie_0530

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object store_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("moxie")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.storeAssignmentPolicy","LEGACY")
      .getOrCreate()

    val order_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info")
      .load()
      .select("id","user_id")

    val order_detail = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_detail")
      .load()
      .select("order_id","sku_id")

    val source = order_info.join(order_detail, order_info("id") === order_detail("order_id"))
      .select("user_id", "sku_id")
      .distinct()

    val res1 = source
      .withColumn("user_id", dense_rank().over(Window.orderBy("user_id")) - 1)
      .withColumn("sku_id", dense_rank().over(Window.orderBy("sku_id")) - 1)
      .orderBy(asc("user_id"), asc("sku_id"))

    println("-------user_id_mapping与sku_id_mapping数据前5条如下：-------")
    res1
      .limit(5)
      .foreach(
        (r) => {
          println(r(0) + ":" + r(1))
        }
      )

    val source2 = res1.withColumn("sku_id", concat(lit("sku_id"), col("sku_id")))

    import spark.implicits._
    val sku_ids = source2.select("sku_id").distinct().orderBy(split(col("sku_id"), "id")(1).cast("int"))
      .map(_(0).toString)
      .collect()

    val res2 = source2.groupBy("user_id")
      .pivot("sku_id", sku_ids)
      .agg(lit(1.0))
      .na.fill(0.0)
      .orderBy("user_id")
      .withColumn("user_id", col("user_id").cast("double"))

    res2.show(5)

    spark.stop()
  }
}
