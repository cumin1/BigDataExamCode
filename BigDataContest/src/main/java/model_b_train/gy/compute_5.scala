package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object compute_5 {
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
      .filter(col("ChangeRecordState") === "运行")
      .filter(col("ChangeEndTime").isNotNull)
      .select("ChangeMachineID","ChangeStartTime","ChangeEndTime")

    val dim_machine = spark.read.format("hudi")
      .load("hdfs://bigdata1:9000/user/hive/warehouse/hudi_gy_dwd.db/dim_machine")
      .select("BaseMachineID","MachineFactory")

    val source = change_record
      .join(dim_machine, change_record("ChangeMachineID") === dim_machine("BaseMachineID"))
      .select(
        dim_machine("BaseMachineID") as "machine_id",
        dim_machine("MachineFactory") as "machine_factory",
        date_format(col("ChangeStartTime"), "yyyy-MM") as "start_month",
        unix_timestamp(col("ChangeEndTime")) - unix_timestamp(col("ChangeStartTime")) as "total_time"
      )
      .groupBy("machine_id","machine_factory","start_month")
      .agg(
        sum("time") as "total_time"
      )

    val frame1 = source
      .groupBy("start_month")
      .agg(
        avg("total_time") as "company_avg"
      )

    val frame2 = source
      .groupBy("start_month", "machine_factory")
      .agg(
        avg("total_time") as "factory_avg"
      )

    val result = frame1.join(frame2, Seq("start_month"))
      .withColumn("comparison",
        when(col("factory_avg") > col("company_avg"), "高")
          .when(col("factory_avg") < col("company_avg"), "低")
          .otherwise("相同")
      )
      .select("start_month", "machine_factory", "comparison", "factory_avg", "company_avg")

    result.createTempView("machine_running_compare")
    spark.sql("select * from machine_running_compare order by machine_factory desc limit 2").show()
  }
}
