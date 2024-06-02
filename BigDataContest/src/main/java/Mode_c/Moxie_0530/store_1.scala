package Mode_c.Moxie_0530

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object store_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("moxie")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
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

//    order_info.show(5)
//    order_detail.show(5)

    val source = order_info.join(order_detail, order_info("id") === order_detail("order_id"))
      .select("user_id", "sku_id")
      .distinct()

//    source.show(5)

    import spark.implicits._
    val sku_ids_6708 = source.filter(col("user_id") === 6708)
      .select("sku_id").distinct().map(_(0).toString.toInt)
      .collect()

//    println(sku_ids_6708.mkString(","))

    val str = source.withColumn("cos",
        when(col("sku_id").isin(sku_ids_6708: _*), 1.0).otherwise(0.0)
      )
      .groupBy("user_id")
      .agg(
        sum("cos") as "same"
      )
      .orderBy(desc("same"))
      .select("user_id")
      .limit(10)
      .map(_(0).toString.toInt)
      .collect()
      .mkString(",")

    println("-------------------相同种类前10的id结果展示为：--------------------")
    println(str)

    val sku_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "sku_info")
      .load()
      .select("id","spu_id","price","weight","tm_id","category3_id" )

    val assembler1 = new VectorAssembler()
      .setInputCols(Array("price"))
      .setOutputCol("price_vec")

    val scaler1 = new StandardScaler()
      .setInputCol("price_vec")
      .setOutputCol("price_stand")

    val assembler2 = new VectorAssembler()
      .setInputCols(Array("weight"))
      .setOutputCol("weight_vec")

    val scaler2 = new StandardScaler()
      .setInputCol("weight_vec")
      .setOutputCol("weight_stand")

    val encoder = new OneHotEncoder()
      .setInputCols(Array("spu_id", "tm_id", "category3_id"))
      .setOutputCols(Array("spu_id_hot", "tm_id_hot", "category3_id_hot"))

    val frame = new Pipeline()
      .setStages(Array(assembler1, scaler1, assembler2, scaler2, encoder))
      .fit(sku_info)
      .transform(sku_info)
      .select("id","price_stand","weight_stand","spu_id_hot", "tm_id_hot", "category3_id_hot")

//    frame.show(5)

    val tran_stand = spark.udf.register("tran_stand", (r: DenseVector) => {
      r.apply(0)
    })
    val tran_hot = spark.udf.register("tran_hot", (r: SparseVector) => {
      r.toArray.mkString(",")
    })

    val res = frame
      .withColumn("price_stand", tran_stand(col("price_stand")))
      .withColumn("weight_stand", tran_stand(col("weight_stand")))
      .withColumn("spu_id_hot", tran_hot(col("spu_id_hot")))
      .withColumn("tm_id_hot", tran_hot(col("tm_id_hot")))
      .withColumn("category3_id_hot", tran_hot(col("category3_id_hot")))

    res.show(5)

    spark.stop()
  }
}
