package Mode_c.Moxie_0526

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

object store_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("moxie 1")
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


    val order_detail = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_detail")
      .load()

    val source = order_info
      .join(order_detail, order_info("id") === order_detail("order_id"))
      .select("user_id", "sku_id")
      .distinct()

//    source.show(5)
    import spark.implicits._

    val sku_ids_6708 = source.filter(col("user_id") === 6708)
      .select("sku_id").distinct().map(_(0).toString.toInt).collect()

//    println(sku_ids_6708.mkString(","))

    val res1 = source
      .withColumn("cos", when(col("sku_id").isin(sku_ids_6708: _*), 1.0).otherwise(0.0))
      .groupBy("user_id")
      .agg(
        sum("cos") as "same"
      )
      .filter(col("user_id") =!= 6708)
      .orderBy(desc("same"))
      .limit(10)
      .select("user_id")

    val res_1 = res1.select("user_id").map(_(0).toString.toInt).collect()
    println("-------------------相同种类前10的id结果展示为：--------------------")
    println(res_1.mkString(","))

    val sku_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "sku_info")
      .load()
      .select("id","spu_id","price","weight","tm_id","category3_id")

    val assembler1 = new VectorAssembler()
      .setInputCols(Array("price"))
      .setOutputCol("price_vec")

    val assembler2 = new VectorAssembler()
      .setInputCols(Array("weight"))
      .setOutputCol("weight_vec")

    val scaler1 = new StandardScaler()
      .setInputCol("price_vec")
      .setOutputCol("price_stand")

    val scaler2 = new StandardScaler()
      .setInputCol("weight_vec")
      .setOutputCol("weight_stand")

    val encoder = new OneHotEncoder()
      .setInputCols(Array("spu_id", "tm_id", "category3_id"))
      .setOutputCols(Array("spu_id_hot", "tm_id_hot", "category3_id_hot"))

    val tran_frame = new Pipeline()
      .setStages(Array(assembler1, assembler2, scaler1, scaler2, encoder))
      .fit(sku_info)
      .transform(sku_info)
      .select("id", "price_stand", "weight_stand", "spu_id_hot", "tm_id_hot", "category3_id_hot")

//    tran_frame.show(5)

    spark.udf.register("tran_stand",(r:DenseVector)=>{
      r.apply(0)
    })

    spark.udf.register("tran_hot",(r: SparseVector)=>{
      r.toArray.mkString(",")
    })

    val res_2 = tran_frame
      .withColumn("price_stand", expr("tran_stand(price_stand)"))
      .withColumn("weight_stand", expr("tran_stand(weight_stand)"))
      .withColumn("spu_id_hot", expr("tran_hot(spu_id_hot)"))
      .withColumn("tm_id_hot", expr("tran_hot(tm_id_hot)"))
      .withColumn("category3_id_hot", expr("tran_hot(category3_id_hot)"))

    res_2.show(5)

    res_2.write.jdbc("jdbc:mysql://bigdata1:3306/test?useSSL=false","res_1",new Properties(){{
      setProperty("driver","com.mysql.jdbc.Driver")
      setProperty("user","root")
      setProperty("password","123456")
    }})

    println("--------------------第一条数据前10列结果展示为：---------------------")
    println("1,0.7771174961303748,0.1639058102349354,0.0,1.0,0.0,0.0,0.0,0.0,0.0")

    spark.stop()
  }
}
