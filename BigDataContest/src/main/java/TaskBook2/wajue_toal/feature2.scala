package TaskBook2.wajue_toal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

import java.util.Properties

object feature2 {
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

    val sku_info = spark.read.jdbc(url, "sku_info", prop)
      .select("id","spu_id","price","weight","tm_id","category3_id")
    sku_info.show()

    val assembler1 = new VectorAssembler()
      .setInputCols(Array("price"))
      .setOutputCol("price_v")
    val assembler2 = new VectorAssembler()
      .setInputCols(Array("weight"))
      .setOutputCol("weight_v")

    val standardScaler1: StandardScaler = new StandardScaler()
      .setInputCol("price_v")
      .setOutputCol("price_sca")
      .setWithMean(true)

    val standardScaler2: StandardScaler = new StandardScaler()
      .setInputCol("weight_v")
      .setOutputCol("weight_sca")
      .setWithMean(true)


    val hotEncoder: OneHotEncoder = new OneHotEncoder()
      .setInputCols(Array("spu_id", "tm_id", "category3_id"))
      .setOutputCols(Array("spu_id_hot", "tm_id_hot", "category3_id_hot"))
      .setDropLast(false)

    val pipelineModel: PipelineModel = new Pipeline()
      .setStages(Array(assembler1,assembler2,standardScaler1,standardScaler2,hotEncoder))
      .fit(sku_info)

    val source1 = pipelineModel.transform(sku_info)

    spark.udf.register("vectorToArray",(v1:SparseVector) =>{
      v1.toArray.mkString(",")
    })

    spark.udf.register("vectorToDouble", (v: DenseVector) => {
      v.apply(0)
    })


    source1
    .withColumn("spu_id_hot",expr("vectorToArray(spu_id_hot)"))
      .withColumn("tm_id_hot",expr("vectorToArray(tm_id_hot)"))
      .withColumn("category3_id_hot",expr("vectorToArray(category3_id_hot)"))
      .select("id","price_sca","weight_sca","spu_id_hot","tm_id_hot","category3_id_hot")
      .withColumn("price_sca",expr("vectorToDouble(price_sca)"))
      .withColumn("weight_sca",expr("vectorToDouble(weight_sca)"))
      .orderBy("id")
      .limit(1)
      .foreach(r =>{
        println(r.toSeq.flatMap(r => r.toString.split(",")).mkString(","))
      })

  }
}
