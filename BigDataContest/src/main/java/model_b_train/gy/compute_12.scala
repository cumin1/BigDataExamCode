package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_12 {
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

    val machine_data = spark.table("dwd.fact_environment_data")
      .withColumn("env_date_year", year(col("InPutTime")))
      .withColumn("env_date_month", month(col("InPutTime")))

    val frame1 = machine_data.groupBy("env_date_year", "env_date_month")
      .agg(
        avg("PM10") as "factory_avg"
      )

    val frame2 = machine_data
      .groupBy("BaseID", "env_date_year", "env_date_month")
      .agg(
        avg("PM10") as "machine_avg"
      )

    val result = frame1.join(frame2, Seq("env_date_year", "env_date_month"))
      .withColumn("comparison",
        when(col("machine_avg") > col("factory_avg"), "高")
          .when(col("machine_avg") < col("factory_avg"), "低")
          .otherwise("相同")
      )
      .withColumnRenamed("BaseID", "base_id")
      .select("base_id", "machine_avg", "factory_avg", "comparison", "env_date_year", "env_date_month")

    result.createOrReplaceTempView("machine_runningAVG_compare")
    spark.sql(
        """
          |select * from machine_runningAVG_compare
          |order by base_id desc
          |limit 5
          |""".stripMargin)
      .show()


  }
}
