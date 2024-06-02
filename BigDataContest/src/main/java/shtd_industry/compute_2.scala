package shtd_industry

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lead, row_number}

object compute_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("9-3")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val frame = spark.table("dws.machine_produce_per_avgtime")
//    frame.show()

    val res = frame
      .withColumn("row", row_number().over(Window.partitionBy("produce_machine_id").orderBy(desc("producetime"))))
      .filter(col("row") <= 2)
      .withColumn("second_time", lead(col("producetime"), 1).over(Window.partitionBy("produce_machine_id").orderBy(desc("producetime"))))
      .filter(col("second_time").isNotNull)
      .withColumnRenamed("produce_machine_id", "machine_id")
      .withColumnRenamed("producetime", "first_time")
      .select("machine_id", "first_time", "second_time")

    res.show()

    res.createOrReplaceTempView("result")
    spark.sql("select * from result order by machine_id desc limit 2").show()

    spark.stop()
  }
}
