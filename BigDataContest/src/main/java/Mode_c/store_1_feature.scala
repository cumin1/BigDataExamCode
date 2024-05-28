package Mode_c

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object store_1_feature {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("电商1 特征工程")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris","thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode","nonstrict").getOrCreate()

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

//    order_info.show(5)
//    order_detail.show(5)

    val source = order_info
      .join(order_detail, order_info("id") === order_detail("order_id"))
      .dropDuplicates("user_id","sku_id")
      .select("user_id", "sku_id")

    val sku_ids_6708 = source.filter(col("user_id") === 6708)
      .select("sku_id").distinct().map(_(0).toString.toInt).collect()

    val feature_1_res = source
      .withColumn("cos",
        when(col("sku_id").isin(sku_ids_6708: _*), 1.0)
          .otherwise(0.0)
      )
      .groupBy("user_id")
      .agg(
        sum("cos") as "same"
      )
      .filter(col("user_id") =!= 6708)
      .orderBy(desc("same"))
      .limit(10)
      .map(_(0).toString.toInt)
      .collect()
      .mkString(",")

    println("-------------------相同种类前10的id结果展示为：--------------------")
    println(feature_1_res)

    // todo 2
    val sku_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "sku_info")
      .load()
      .select("id","price","weight","spu_id","tm_id","category3_id")

    sku_info.show(5)
    // 问题：要对price weight 四个字段做规范化
    // 问题：要对spu_id tm_id category3_id 做one-hot编码
    // todo 首先做个规范化  规范化简单
    val vectorAssembler1 = new VectorAssembler()
      .setInputCols(Array("price"))
      .setOutputCol("price_v")

    val vectorAssembler2 = new VectorAssembler()
      .setInputCols(Array("weight"))
      .setOutputCol("weight_v")

    val standardScaler1 = new StandardScaler()
      .setInputCol("price_v")
      .setOutputCol("price_sca")
//      .setWithStd(true)

    val standardScaler2 = new StandardScaler()
      .setInputCol("weight_v")
      .setOutputCol("weight_sca")
//      .setWithStd(true)

    // todo 再做one-hot编码
    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(Array("spu_id", "tm_id", "category3_id"))
      .setOutputCols(Array("spu_id_hot", "tm_id_hot", "category3_id_hot"))
      .setDropLast(false)


    spark.udf.register("tran_hot",(v1:SparseVector) =>{
      v1.toArray.mkString(",")
    })

    spark.udf.register("train_sca", (v: DenseVector) => {
      v.apply(0)
    })
    val df1 = new Pipeline()
      .setStages(Array(vectorAssembler1, vectorAssembler2, standardScaler1, standardScaler2, oneHotEncoder))
      .fit(sku_info)
      .transform(sku_info)
      .select("id", "price_sca", "weight_sca", "spu_id_hot", "tm_id_hot", "category3_id_hot")

    df1.show(5)

    val model = df1
      .withColumn("spu_id_hot",expr("tran_hot(spu_id_hot)"))
      .withColumn("tm_id_hot",expr("tran_hot(tm_id_hot)"))
      .withColumn("category3_id_hot",expr("tran_hot(category3_id_hot)"))

    model.show(5)

    val feature_2_res = model
      .withColumn("price_sca", expr("train_sca(price_sca)"))
      .withColumn("weight_sca", expr("train_sca(weight_sca)"))

    feature_2_res.show(5)

    feature_2_res.write.format("hive").mode("overwrite").saveAsTable("dws.store_1_feature_res")

    println("---------------第一行前5列结果展示为---------------")
    feature_2_res
      .orderBy(asc("id"))
      .limit(1)
    // 发现是 1,-0.9710484974908347,-1.5412930671303393,0.0,1.0

    println("1,0.7771174961303748,0.1639058102349354,0.0,1.0")



  }
}
