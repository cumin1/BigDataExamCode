package shtd_industry

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object compute_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("9-2")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val source = spark.table("dwd.fact_produce_record")
//    source.show(5)

    val data = source
      .filter(col("producecodeendtime") =!= "1900-01-01 00:00:00")
      .dropDuplicates("ProduceRecordID", "ProduceMachineID")
      .select("ProduceRecordID", "ProduceMachineID", "ProduceCodeStartTime", "ProduceCodeEndTime")
      .withColumn("producetime", unix_timestamp(col("ProduceCodeEndTime")) - unix_timestamp(col("ProduceCodeStartTime")))

    val res = data
      .groupBy("ProduceMachineID")
      .agg(
        avg("producetime").as("produce_per_avgtime")
      )
      .join(data, Seq("ProduceMachineID"))
//      .filter(col("producetime") > col("produce_per_avgtime"))
      .withColumnRenamed("ProduceMachineID", "produce_machine_id")
      .withColumnRenamed("ProduceRecordID", "produce_record_id")
      .withColumn("produce_per_avgtime",col("produce_per_avgtime").cast("bigint"))
      .select("produce_record_id", "produce_machine_id", "producetime", "produce_per_avgtime")

    res.createOrReplaceTempView("result")
    spark.sql("select * from result order by produce_machine_id desc limit 3").show()

    res.write.format("hive").mode("append").saveAsTable("dws.machine_produce_per_avgtime")


    spark.stop()
  }
}
