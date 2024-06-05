package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_6 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .getOrCreate()

    val change_record = spark.read.format("hudi")
      .load("hdfs://bigdata1:9000/user/hive/warehouse/hudi_gy_dwd.db/fact_change_record")
      .select("ChangeMachineID","ChangeRecordState","ChangeStartTime","ChangeEndTime")

    val result = change_record
      .withColumn("row",
        row_number()
          .over(Window.partitionBy("ChangeMachineID").orderBy(desc("ChangeStartTime")))
      )
      .filter(col("row") <= 2)
      .withColumn("row",
        row_number()
          .over(Window.partitionBy("ChangeMachineID").orderBy(asc("ChangeStartTime")))
      )
      .filter(col("row") === 1)
      .withColumnRenamed("ChangeMachineID", "machine_id")
      .withColumnRenamed("ChangeRecordState", "record_state")
      .withColumnRenamed("ChangeStartTime", "change_start_time")
      .withColumnRenamed("ChangeEndTime", "change_end_time")
      .select("machine_id", "record_state", "change_start_time", "change_end_time")

    result.createOrReplaceTempView("recent_state")
    spark.sql(
      """
        |select * from recent_state
        |order by machine_id desc
        |limit 5
        |""".stripMargin).show()
  }
}
