package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object compute_2 {
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
      .withColumn("time",unix_timestamp(col("ChangeEndTime"))-unix_timestamp(col("ChangeStartTime")))
      .select("ChangeMachineID","time")


    val dim_machine = spark.read.format("hudi")
      .load("hdfs://bigdata1:9000/user/hive/warehouse/hudi_gy_dwd.db/dim_machine")
      .select("BaseMachineID","MachineFactory")

    val source = change_record
      .join(dim_machine, change_record("ChangeMachineID") === dim_machine("BaseMachineID"))
      .withColumnRenamed("ChangeMachineID", "machine_id")
      .withColumnRenamed("MachineFactory", "machine_factory")
      .select("machine_id", "machine_factory", "time")
      .groupBy("machine_id","machine_factory")
      .agg(
        sum("time") as "total_running_time"
      )

    val medain_num = spark.udf
      .register("medain_num", (ids: Array[Int], times: Array[Int]) => {
      var r = ""

      if (times.length % 2 == 0) {
        r = ids((times.length / 2) - 1) + "," + ids(times.length / 2)
      } else {
        r = ids(times.length / 2).toString
      }

      r
    })

    import spark.implicits._

    val medain_ids = source
      .orderBy("total_running_time")
      .groupBy("machine_factory")
      .agg(
        collect_list(col("machine_id")) as "id_list",
        collect_list(col("total_running_time")) as "time_list"
      )
      .withColumn("medain_ids", medain_num(col("id_list"), col("time_list")))
      .select("medain_ids")
      .map(_(0).toString)
      .flatMap(_.split(","))
      .collect()

    println(medain_ids.mkString(","))

    val result = source.filter(col("machine_id").isin(medain_ids: _*))
      .select("machine_id", "machine_factory", "total_running_time")

    result.createOrReplaceTempView("machine_running_median")
    spark.sql(
      """
        |select * from machine_running_median
        |order by machine_factory desc,machine_id desc
        |limit 5
        |""".stripMargin).show()
  }
}
