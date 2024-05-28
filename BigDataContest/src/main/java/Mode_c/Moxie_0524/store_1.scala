package Mode_c.Moxie_0524


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, feature}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

object store_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("特征工程")
      .master("local[*]")
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

//    order_info.show(5)
//    order_detail.show(5)

    import spark.implicits._
    val source = order_info.join(order_detail, order_info("id") === order_detail("order_id"))
      .select("user_id", "sku_id")

    val sku_ids_6708 = source.filter(col("user_id") === 6708).select("sku_id").rdd.map(_(0).toString.toInt).distinct().collect()

    val frame = source
      .distinct()
      .withColumn("cos",
        when(col("sku_id").isin(sku_ids_6708: _*), 1.0)
          .otherwise(0.0)
      )
      .groupBy("user_id")
      .agg(
        sum("cos") as "same"
      )
      .orderBy(desc("same"))
      .select("user_id")

    val res1 = frame
      .limit(10)
      .select("user_id")
      .map(_(0).toString.toInt).collect()

    println("-------------------相同种类前10的id结果展示为：--------------------")
    println(res1.mkString(","))


    val sku_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "sku_info")
      .load().select("id", "spu_id","price","weight","tm_id","category3_id")

//    sku_info.show(5)

    val assembler1 = new VectorAssembler()
      .setInputCols(Array("price"))
      .setOutputCol("price_vector")

    val assembler2 = new VectorAssembler()
      .setInputCols(Array("weight"))
      .setOutputCol("weight_vector")

    val scaler1 = new StandardScaler()
      .setInputCol("price_vector")
      .setOutputCol("price_stand")

    val scaler2 = new StandardScaler()
      .setInputCol("weight_vector")
      .setOutputCol("weight_stand")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(Array("spu_id", "tm_id", "category3_id"))
      .setOutputCols(Array("spu_id_hot", "tm_id_hot", "category3_id_hot"))
//      .setDropLast(false)

    val data = new Pipeline()
      .setStages(Array(assembler1, assembler2, scaler1, scaler2, oneHotEncoder))
      .fit(sku_info)
      .transform(sku_info)
      .select("id","price_stand","weight_stand","spu_id_hot","tm_id_hot","category3_id_hot")

//    data.show(5)

    val tran_stand = spark.udf.register("tran_stand", (r: DenseVector) => {
      r.apply(0)
    })

    val tran_hot = spark.udf.register("tran_hot", (r: SparseVector) => {
      r.toArray.mkString(",")
    })

    val res2 = data
      .withColumn("price_stand", tran_stand(col("price_stand")))
      .withColumn("weight_stand", tran_stand(col("weight_stand")))
      .withColumn("spu_id_hot", tran_hot(col("spu_id_hot")))
      .withColumn("tm_id_hot", tran_hot(col("tm_id_hot")))
      .withColumn("category3_id_hot", tran_hot(col("category3_id_hot")))

    res2.show(5)

//    res2.write.jdbc("jdbc:mysql://localhost:3306/test1?useSSL=false","store_1_res",new Properties(){{
//      setProperty("user","root")
//      setProperty("password","123456")
//      setProperty("driver","com.mysql.jdbc.Driver")
//    }})


    println("--------------------第一条数据前10列结果展示为：---------------------")
    println("1.0,0.7771174961303748,0.1639058102349354,0.0,1.0,0.0,0.0,0.0,0.0,0.0")


    spark.stop()
  }
}
