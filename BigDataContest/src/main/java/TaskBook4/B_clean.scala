package TaskBook4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._

object B_clean {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书4 清洗")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val nowTime = date_format(date_sub(current_timestamp(),1),"yyyy-MM-dd HH:mm:ss")

//    spark.sql("create database hudi_gy_dwd")

    //(1)
    val changerecord = spark.table("hudi_gy_ods.changerecord")

    val result1 = changerecord
      .drop("etldate")
      .withColumn("ChangeStartTime", date_format(col("ChangeStartTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("ChangeEndTime", date_format(col("ChangeEndTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240425"))



    // (2)
    val basemachine = spark.table("hudi_gy_ods.basemachine")
    val result2 = basemachine
      .drop("etldate")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240425"))

    // (3)
    val producerecord = spark.table("hudi_gy_ods.producerecord")
    val result3 = producerecord
      .withColumn("ProduceStartWaitTime", date_format(col("ProduceStartWaitTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("ProduceCodeStartTime", date_format(col("ProduceCodeStartTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("ProduceCodeEndTime", date_format(col("ProduceCodeEndTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("ProduceEndTime", date_format(col("ProduceEndTime"), "yyyy-MM-dd HH:mm:ss"))
      .drop("etldate")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240425"))

    // (4)
    val machinedata = spark.table("hudi_gy_ods.machinedata")
    val result4 = machinedata
      .drop("etldate")
      .withColumn("MachineRecordDate", date_format(col("MachineRecordDate"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240425"))

    write_to_hudi(
      result1,"hudi_gy_dwd","fact_change_record","ChangeID,ChangeMachineID","dwd_modify_time","etldate",
      "/user/hive/warehouse/hudi_gy_dwd.db/fact_change_record"
    )

    write_to_hudi(
      result2,"hudi_gy_dwd","dim_machine","BaseMachineID","dwd_modify_time","etldate",
      "/user/hive/warehouse/hudi_gy_dwd.db/dim_machine"
    )

    write_to_hudi(
      result3,"hudi_gy_dwd","fact_produce_record","ProduceRecordID,ProduceMachineID","dwd_modify_time","etldate",
      "/user/hive/warehouse/hudi_gy_dwd.db/fact_produce_record"
    )

    write_to_hudi(
      result4,"hudi_gy_dwd","fact_machine_data","MachineRecordID","dwd_modify_time","etldate",
      "/user/hive/warehouse/hudi_gy_dwd.db/fact_machine_data"
    )

    def write_to_hudi(df:DataFrame,databasename:String,tablename:String,recordkey:String,precombinekey:String,partitionkey:String,savePath:String): Unit = {
      df
        .write
        .format("hudi")
        .mode("append")
        .option("hoodie.table.name",tablename)
        .option(RECORDKEY_FIELD.key(),recordkey)
        .option(PRECOMBINE_FIELD.key(),precombinekey)
        .option(PARTITIONPATH_FIELD.key(),partitionkey)
        .option("hoodie.datasource.write.hive_style_partitioning","true")
        .save(savePath)


      spark.sql(s"create table ${databasename}.${tablename} using hudi location '${savePath}'")
      spark.sql(s"msck repair table ${databasename}.${tablename}")
      spark.sql(s"show partitions  ${databasename}.${tablename}").show()
      println(tablename,"的抽取已完成！！")
    }
  }
}
