package TaskBook5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object B_clean {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书5 清洗")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()
    val nowTime = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")

    val changerecord = spark.table("ods.changerecord")

    val result1 = changerecord
      .drop("etldate")
      .withColumn("changestarttime", date_format(col("changestarttime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("changeendtime", date_format(col("changeendtime"), "yyyy-MM-dd HH:mm:ss"))
      .dropDuplicates("changeid", "changemachineid")
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(nowTime))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(nowTime))
      .withColumn("etldate", lit("20240426"))

    result1.write.format("hive").mode("append").partitionBy("etldate").saveAsTable("dwd.fact_change_record")
    spark.sql("select * from dwd.fact_change_record order by changemachineid desc limit 1").show()

    val basemachine = spark.table("ods.basemachine")
    val result2 = basemachine
      .drop("etldate")
      .dropDuplicates("basemachineid")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240426"))

    result2.write.format("hive").mode("append").partitionBy("etldate").saveAsTable("dwd.dim_machine")
    spark.sql("select * from dwd.dim_machine order by basemachineid asc limit 2").show()


    val producerecord = spark.table("ods.producerecord")
    val result3 = producerecord
      .drop("etldate")
      .withColumn("producestartwaittime", date_format(col("producestartwaittime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("producecodestarttime", date_format(col("producecodestarttime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("producecodeendtime", date_format(col("producecodeendtime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("produceendtime", date_format(col("produceendtime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240426"))

    result3.write.format("hive").mode("append").partitionBy("etldate").saveAsTable("dwd.fact_produce_record")
    spark.sql("select * from dwd.fact_produce_record order by producemachineid asc limit 1").show()

    val machinedata = spark.table("ods.machinedata")
    val result4 = machinedata
      .drop("etldate")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240426"))

    result4.write.format("hive").mode("append").partitionBy("etldate").saveAsTable("dwd.fact_machine_data")
    spark.sql("select * from dwd.fact_machine_data order by machineid desc limit 1").show()

    spark.stop()
  }
}
