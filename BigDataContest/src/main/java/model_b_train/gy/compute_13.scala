package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_13 {
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

    val machine_data = spark.read.format("hudi")
      .load("hdfs://bigdata1:9000/user/hive/warehouse/hudi_gy_dwd.db/fact_machine_data")


    val result = machine_data
      .select("MachineID", "MachineRecordState", "MachineRecordDate")
      .withColumn("next_time",
        lead(col("MachineRecordDate"), 1).over(Window.partitionBy("MachineID").orderBy(desc("MachineRecordDate")))
      )
      .filter(col("MachineRecordState") === "运行")
      .withColumn("time", abs(unix_timestamp(col("next_time")) - unix_timestamp(col("MachineRecordDate"))))
      .withColumn("machine_record_date", date_format(col("MachineRecordDate"), "yyyy-MM-dd"))
      .groupBy("MachineID", "machine_record_date")
      .agg(
        sum("time") as "total_time"
      )
      .withColumnRenamed("MachineID", "machine_id")
      .select("machine_id", "machine_record_date", "total_time")

    result.createOrReplaceTempView("machine_data_total_time")
    spark.sql(
      """
        |select * from machine_data_total_time
        |order by machine_id desc,machine_record_date asc
        |limit 5
        |""".stripMargin).show()

  }
}
