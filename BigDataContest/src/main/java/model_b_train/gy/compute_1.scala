package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object compute_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .getOrCreate()

    val data = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/hudi_gy_dwd.db/fact_change_record")
      .filter(col("ChangeEndTime").isNotNull)
      .select("ChangeMachineID","ChangeRecordState","ChangeStartTime","ChangeEndTime")

    val result = data
      .withColumn("year", year(col("ChangeStartTime")))
      .withColumn("month", month(col("ChangeStartTime")))
      .withColumn("time", unix_timestamp(col("ChangeEndTime")) - unix_timestamp(col("ChangeStartTime")))
      .withColumnRenamed("ChangeMachineID", "machine_id")
      .withColumnRenamed("ChangeRecordState", "change_record_state")
      .groupBy("machine_id", "change_record_state", "year", "month")
      .agg(
        sum("time") as "duration_time"
      )
      .select("machine_id", "change_record_state", "duration_time", "year", "month")

    result.createOrReplaceTempView("machine_state_time")
    spark.sql(
      """
        |select * from machine_state_time
        |order by machine_id desc,duration_time desc
        |limit 10
        |""".stripMargin).show()

    spark.stop()
  }
}
