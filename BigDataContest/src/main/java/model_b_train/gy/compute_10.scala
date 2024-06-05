package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_10 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris","thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .getOrCreate()

    val data = spark.table("dwd.fact_produce_record")
      .filter(col("producecodeendtime") =!= "1900-01-01 00:00:00")
      .dropDuplicates("ProduceRecordID","ProduceMachineID")
      .withColumn("producetime",unix_timestamp(col("producecodeendtime"))-unix_timestamp(col("producecodestarttime")))
      .select("ProduceRecordID","ProduceMachineID","producetime")

    val frame = data
      .groupBy("ProduceMachineID")
      .agg(
        avg("producetime") as "produce_per_avgtime"
      )

    val result = data.join(frame, Seq("ProduceMachineID"))
      .withColumnRenamed("ProduceRecordID", "produce_record_id")
      .withColumnRenamed("ProduceMachineID", "produce_machine_id")
      .select("produce_record_id", "produce_machine_id", "producetime", "produce_per_avgtime")
      .withColumn("produce_per_avgtime",col("produce_per_avgtime").cast("int"))

    result.createOrReplaceTempView("machine_produce_per_avgtime")
    spark.sql(
      """
        |select * from machine_produce_per_avgtime
        |order by produce_machine_id desc
        |limit 3
        |""".stripMargin).show()

  }
}
