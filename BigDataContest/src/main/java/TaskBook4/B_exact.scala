package TaskBook4

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties


object B_exact {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书4 抽取")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    prop.put("driver","com.mysql.jdbc.Driver")

    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false"

    // (1)
    val mysql_change_record = spark.read.jdbc(mysql_url, "ChangeRecord", prop)
    val result1 = mysql_change_record
      .withColumn("etldate", lit("20240425"))

    // (2)
    val mysql_BaseMachine = spark.read.jdbc(mysql_url, "BaseMachine", prop)
    val result2 = mysql_BaseMachine
      .withColumn("etldate", lit("20240425"))

    // (3)
    val mysql_ProduceRecord = spark.read.jdbc(mysql_url, "ProduceRecord", prop)
    val result3 = mysql_ProduceRecord
      .drop("ProducePrgCode")
      .withColumn("etldate", lit("20240425"))

    // (4)
    val mysql_MachineData = spark.read.jdbc(mysql_url, "MachineData", prop)
    val result4 = mysql_MachineData
      .withColumn("etldate", lit("20240425"))



    writeDataToHudi(result1,"changerecord","/user/hive/warehouse/hudi_gy_ods.db/changerecord","hudi_gy_ods","ChangeID,ChangeMachineID","ChangeEndTime")

    writeDataToHudi(result2,"basemachine","/user/hive/warehouse/hudi_gy_ods.db/basemachine","hudi_gy_ods","BaseMachineID","MachineAddDate")

    writeDataToHudi(result3,"producerecord","/user/hive/warehouse/hudi_gy_ods.db/producerecord","hudi_gy_ods","ProduceRecordID,ProduceMachineID","ProduceCodeEndTime")

    writeDataToHudi(result4,"machinedata","/user/hive/warehouse/hudi_gy_ods.db/machinedata","hudi_gy_ods","MachineRecordID","MachineRecordDate")

    spark.stop()
    def writeDataToHudi(dataFrame: DataFrame, hudiTableName: String, hudiTablePath: String,database :String,recordkey:String,recombine_key :String): Unit = {
      dataFrame.write.format("org.apache.hudi")
        .option("hoodie.table.name", hudiTableName)
        .option(PRECOMBINE_FIELD.key(), recombine_key)
        // TODO 联合主键
        .option(RECORDKEY_FIELD.key(), recordkey)
        .option(PARTITIONPATH_FIELD.key(), "etldate")
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.datasource.write.allowNullValue", "true") // 允许写入空值
        .mode("append") // 写入模式，可以是 overwrite 或 append
        .save(hudiTablePath)

      spark.sql(s"create table ${database}.${hudiTableName} using hudi location '${hudiTablePath}'")
      spark.sql(s"msck repair table ${database}.${hudiTableName}")
      spark.sql(s"show partitions ${database}.${hudiTableName}").show()
    }

  }
}
