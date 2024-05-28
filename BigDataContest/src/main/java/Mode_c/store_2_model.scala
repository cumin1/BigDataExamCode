package Mode_c

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, SingularValueDecomposition, Vectors, distributed}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.mutable.ArrayBuffer


object store_2_model {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("抽取")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    import spark.implicits._

    val data = spark.table("dws.store_2_feature_res")
    data.show(5)


    val seq = data.filter(col("user_id") === 0.0).limit(1)
      .map(_.toSeq.map(_.toString.toDouble))
      .collect()(0)

    println(seq)  // List(0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0)

    val skus = seq.slice(1, seq.length) // 这里把第一列组成的列表的第一个元素（user_id的值）去掉了
    println(skus)    // List(0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0)

    val buy = new ArrayBuffer[Int]()
    val nobuy = new ArrayBuffer[Int]()

    skus.zipWithIndex.foreach(e => {   // zipWithIndex能把数组变为(value,count)的形式
      if (e._1.equals(0.0)){   // 这里取第一个值也就是value去和0.0比
        nobuy += e._2    // 将第二个值 count 这里对于商品的id 0 1 2 3
      }else{
        buy += e._2
      }
    })

    println(buy)   // ArrayBuffer(3, 4, 11, 12)
    println(nobuy)   // ArrayBuffer(0, 1, 2, 5, 6, 7, 8, 9, 10, 13, 14, 15)

    // todo 执行奇异值分解（SVD）和相似度计算
    val array = data.rdd.map(r => {
      val vector: linalg.Vector = Vectors.dense(
        r.toSeq.slice(1, r.toSeq.length).map(
          (_: Any).toString.toDouble
        ).toArray
      )
      vector
    }).collect()  // 这里每一行的数据去掉第一列 剩下的列组成一个新的数组

    // println(array.mkString(","))  // 这里保存了一个 包含原数据每一行去掉第一列的新数组

    val rowMatrix = new RowMatrix(spark.sparkContext.parallelize(array))  // 这里把上面的大数组 变成了矩阵
    val svd = rowMatrix.computeSVD(5, computeU = true)  // 这里计算矩阵前5列的svd值  返回U矩阵
    val v = svd.V  // 返回V矩阵
    val toArray: Array[(linalg.Vector,Int)] = v.rowIter.zipWithIndex.toArray

    // println(toArray.mkString(","))
    // ([-0.24652240210580767,0.19715912159260437,-0.4106123748339298,0.3054588860074803,0.20256736110526227],0),
    // ([-0.24599510462352803,-0.09501290434427553,0.2610521523375759,-0.13153570052620955,0.17086985649264166],1),
    // ([-0.2530150715154571,-0.32268387327916037,0.15670574263580908,0.2843408486627328,0.23530596097721793],2),
    // ([-0.24950283529913997,0.14507578263674667,-0.03218726544500235,-0.028417988954182988,-0.3559948076934925],3),
    // ([-0.24903760494678423,0.3827470985335942,-0.05928213856183233,-0.21944912248212153,-0.1988136295259452],4),
    // ([-0.25239581211506446,-0.10851409400435476,0.19840753280163786,0.31831161503432936,-0.3690579893266571],5),
    // ([-0.2538707231942369,-0.09531102001593297,-0.113801210562568,-0.029946386564458777,-0.03686410142016246],6)

    val cos: UserDefinedFunction = spark.udf.register("cos", (v1: DenseVector, v2: DenseVector) => {
      1 - breeze.linalg.functions.cosineDistance(breeze.linalg.DenseVector(v1.toArray), breeze.linalg.DenseVector(v2.toArray))
    }) // 计算余弦相似度的自定义函数

    val sku_info: DataFrame = spark.createDataFrame(
      toArray
    ).toDF("vec", "sku_mapping_id")

    sku_info.show()

    sku_info.crossJoin(sku_info.withColumnRenamed("sku_mapping_id", "sku_mapping_id2")
        .withColumnRenamed("vec", "vec2")
      )
      .withColumn("cos", cos(col("vec"), col("vec2")))
      .filter(col("sku_mapping_id") !== col("sku_mapping_id2"))
      .filter(col("sku_mapping_id").isin(nobuy: _*) && col("sku_mapping_id2").isin(buy: _*))
      .groupBy("sku_mapping_id")
      .agg(
        avg("cos").as("avg_cos")
      )
      .orderBy(desc("avg_cos"))
      .show()

    spark.stop()
  }
}
