package TaskBook5

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
      .appName("任务书5 抽取")
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
    prop.put("user","root")
    prop.put("password","123456")
    prop.put("driver","com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false"


    // ChangeRecord，BaseMachine，MachineData， ProduceRecord全量抽取到Hive的
    // ods库中对应表changerecord，basemachine， machinedata，producerecord中。

    exactToHive("ChangeRecord","changerecord")
    exactToHive("BaseMachine","basemachine")
    exactToHive("MachineData","machinedata")
    exactToHive("ProduceRecord","producerecord")

    def exactToHive(mysql_table_name:String,hive_table_name:String): Unit = {
      val mysql_table = spark.read.jdbc(url, mysql_table_name, prop)
      var hive_table = mysql_table
        .withColumn("etldate", lit("20240426"))

      if (mysql_table_name == "ProduceRecord"){
        hive_table = hive_table
          .drop("ProducePrgCode")
      }

      hive_table
        .write
        .format("hive")
        .mode("append")
        .partitionBy("etldate")
        .saveAsTable(s"ods.${hive_table_name}")
      println(mysql_table_name + " ---> " + hive_table_name," 抽取完成!!")
    }

    spark.stop()

  }
}
