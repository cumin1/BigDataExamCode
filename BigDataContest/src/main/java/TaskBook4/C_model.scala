package TaskBook4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

object C_model {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("任务书4 数据挖掘").master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    import spark.implicits._

    val dataFrame = spark.table("dwd.fact_machine_learning_data")
    val cols: Array[String] = dataFrame.columns.slice(3, dataFrame.columns.length - 5)
    val Array(ranting,test) = dataFrame.na.fill(0.0,cols).withColumnRenamed("machine_record_state","label").randomSplit(Array(0.8, 0.2))
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")

    val classifier: RandomForestClassifier = new RandomForestClassifier()

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(assembler,classifier))


    val paramMaps: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(classifier.numTrees, Array(10, 20, 30))
      .addGrid(classifier.maxDepth, Array(5, 10, 15))
      .addGrid(classifier.impurity, Array("entropy", "gini"))
      .build()

    val crossValidator: CrossValidator = new CrossValidator()
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(new MulticlassClassificationEvaluator())
      .setEstimator(pipeline)
      .setParallelism(3)

    val result: DataFrame = crossValidator.fit(ranting).transform(test)

    result.show()
  }
}
