package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_11 {
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

    val data = spark.table("dws.machine_produce_per_avgtime")
//    data.show()

    val result = data
      .select("produce_machine_id", "producetime")
      .withColumn("second_time",
        lead("producetime", 1).over(Window.partitionBy("produce_machine_id").orderBy(desc("producetime")))
      )
      .withColumn("row",
        row_number().over(Window.partitionBy("produce_machine_id").orderBy(desc("producetime")))
      )
      .filter(col("row") === 1)
      .drop("row")
      .withColumnRenamed("producetime", "first_time")
      .withColumnRenamed("produce_machine_id", "machine_id")

    result.createOrReplaceTempView("machine_produce_timetop2")
    spark.sql(
      """
        |select * from machine_produce_timetop2
        |order by machine_id desc
        |limit 2
        |""".stripMargin).show()


  }
}
