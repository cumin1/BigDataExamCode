package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_8 {
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

    val fact_record = spark.table("dwd.fact_change_record")
      .filter(col("changeendtime").isNotNull)
      .withColumn("time",unix_timestamp(col("changeendtime")) - unix_timestamp(col("changestarttime")))
      .withColumn("year",year(col("changestarttime")))
      .withColumn("month",month(col("changestarttime")))
      .select("ChangeMachineID","ChangeRecordState","time","year","month")


    val result = fact_record.groupBy("year", "month", "ChangeMachineID", "ChangeRecordState")
      .agg(sum("time") as "duration_time")
      .withColumnRenamed("ChangeMachineID", "machine_id")
      .withColumnRenamed("ChangeRecordState", "change_record_state")
      .select("machine_id", "change_record_state", "duration_time", "year", "month")

    result.createOrReplaceTempView("machine_state_time")
    spark.sql(
      """
        |select * from machine_state_time
        |order by machine_id desc,duration_time desc
        |limit 10
        |""".stripMargin).show()

  }
}
