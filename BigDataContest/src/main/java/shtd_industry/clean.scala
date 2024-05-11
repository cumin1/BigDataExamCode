package shtd_industry

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object clean {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("四和天地实训 清洗")
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

    val changerecord = spark.read.format("hudi").load("/user/hive/warehouse/hudi_gy_ods.db/changerecord")
    val fact_change_record = changerecord.drop("etldate")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240501"))

    val basemachine = spark.read.format("hudi").load("/user/hive/warehouse/hudi_gy_ods.db/basemachine")
    val dim_machine = basemachine.drop("etldate")
      .withColumn("MachineAddDate",date_format(col("MachineAddDate"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240501"))

    val producerecord = spark.read.format("hudi").load("/user/hive/warehouse/hudi_gy_ods.db/producerecord")
    val fact_produce_record = producerecord.drop("etldate")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240501"))

    val machinedata = spark.read.format("hudi").load("/user/hive/warehouse/hudi_gy_ods.db/machinedata")
    val fact_machine_data = machinedata.drop("etldate")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", lit(nowTime))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", lit(nowTime))
      .withColumn("etldate", lit("20240501"))


//    WriteToHudi(fact_change_record,"hudi_gy_dwd","fact_change_record","dwd_modify_time","ChangeID,ChangeMachineID",
//    "/user/hive/warehouse/hudi_gy_dwd.db/fact_change_record")
//    WriteToHudi(dim_machine,"hudi_gy_dwd","dim_machine","dwd_modify_time","BaseMachineID",
//      "/user/hive/warehouse/hudi_gy_dwd.db/dim_machine")
    WriteToHudi(fact_produce_record,"hudi_gy_dwd","fact_produce_record","dwd_modify_time","ProduceRecordID,ProduceMachineID",
      "/user/hive/warehouse/hudi_gy_dwd.db/fact_produce_record")
    WriteToHudi(fact_machine_data,"hudi_gy_dwd","fact_machine_data","dwd_modify_time","MachineRecordID",
      "/user/hive/warehouse/hudi_gy_dwd.db/fact_machine_data")

    def WriteToHudi(df:DataFrame,hudidatabase:String,huditable:String,precombinekey:String,recordkey:String,savepath:String): Unit = {
      df.write.format("hudi").mode("append")
        .option("hoodie.table.name",huditable)
        .option(PRECOMBINE_FIELD.key(),precombinekey)
        .option(RECORDKEY_FIELD.key(),recordkey)
        .option(PARTITIONPATH_FIELD.key(),"etldate")
        .option("hoodie.datasource.write.hive_style_partitioning","true")
        .save(savepath)

      println(s"${hudidatabase}.${huditable}数据写入成功!!")

      spark.sql(s"create database if not exists ${hudidatabase}")
      spark.sql(s"create table ${hudidatabase}.${huditable} using hudi location '${savepath}'")
      spark.sql(s"msck repair table ${hudidatabase}.${huditable}")

      println("该表修复分区成功！！")

      spark.sql(s"show partitions ${hudidatabase}.${huditable}").show()
    }
  }
}
