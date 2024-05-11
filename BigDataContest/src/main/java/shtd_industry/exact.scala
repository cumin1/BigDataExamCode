package shtd_industry

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._


import java.util.Properties

object exact {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("四和天地实训 抽取")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","123456")
    val url = "jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false"

    // (1)
    val ChangeRecord = spark.read.jdbc(url, "ChangeRecord", prop)
    val ods_changerecord = ChangeRecord.withColumn("etldate", lit("20240501"))

    // (2)
    val BaseMachine = spark.read.jdbc(url, "BaseMachine", prop)
    val ods_basemachine = BaseMachine.withColumn("etldate", lit("20240501"))

    // (3)
    val ProduceRecord = spark.read.jdbc(url, "ProduceRecord", prop)
    val ods_producerecord = ProduceRecord.drop("ProducePrgCode").withColumn("etldate", lit("20240501"))

    // (4)
    val MachineData = spark.read.jdbc(url, "MachineData", prop)
    val ods_machinedata = MachineData.withColumn("etldate", lit("20240501"))

    WriteToHudi(ods_changerecord,"hudi_gy_ods","changerecord","ChangeEndTime","ChangeID,ChangeMachineID",
      "/user/hive/warehouse/hudi_gy_ods.db/changerecord")
    WriteToHudi(ods_basemachine,"hudi_gy_ods","basemachine","MachineAddDate","BaseMachineID",
      "/user/hive/warehouse/hudi_gy_ods.db/basemachine")
    WriteToHudi(ods_producerecord,"hudi_gy_ods","producerecord","ProduceCodeEndTime","ProduceRecordID,ProduceMachineID",
      "/user/hive/warehouse/hudi_gy_ods.db/producerecord")
    WriteToHudi(ods_machinedata,"hudi_gy_ods","machinedata","MachineRecordDate","MachineRecordID",
      "/user/hive/warehouse/hudi_gy_ods.db/machinedata")

    spark.stop()
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
