package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_15 {
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

    val produce_record = spark.read.format("hudi")
      .load("hdfs://bigdata1:9000/user/hive/warehouse/hudi_gy_dwd.db/fact_produce_record")

    val source = produce_record.filter(col("ProduceCodeEndTime") =!= "1900-01-01 00:00:00")
      .dropDuplicates("ProduceRecordID", "ProduceMachineID")
      .select("ProduceRecordID", "ProduceMachineID", "ProduceCodeStartTime", "ProduceCodeEndTime")
      .withColumn("producetime", unix_timestamp(col("ProduceCodeEndTime")) - unix_timestamp(col("ProduceCodeStartTime")))

    val frame1 = source
      .groupBy("ProduceMachineID")
      .agg(
        avg("producetime") as "produce_per_avgtime"
      )

    val result = source.join(frame1, Seq("ProduceMachineID"))
      .withColumnRenamed("ProduceRecordID", "produce_record_id")
      .withColumnRenamed("ProduceMachineID", "produce_machine_id")
      .filter(col("producetime") > col("produce_per_avgtime"))
      .withColumn("produce_per_avgtime",col("produce_per_avgtime").cast("int"))
      .select("produce_record_id", "produce_machine_id", "producetime", "produce_per_avgtime")

    result.createOrReplaceTempView("machine_produce_per_avgtime")
    spark.sql(
      """
        |select * from machine_produce_per_avgtime
        |order by produce_machine_id desc
        |limit 3
        |""".stripMargin).show()

  }
}
