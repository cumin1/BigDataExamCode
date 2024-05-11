package TaskBook4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{current_date, date_format, date_sub}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dom4j.DocumentHelper

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties



object C_feature {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书4 数据挖掘")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    import spark.implicits._

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    prop.put("driver","com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false"

    val data: DataFrame = spark.read.jdbc("jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false", "MachineData", prop)

//    data.show()

    val source = data.map(r => {
      val m: machine = machine()
      m.machine_record_id = r(0).toString.toInt
      m.machine_id = r(1).toString.toDouble
      m.machine_record_state = if (r(2).toString.equals("报警")) 1.0 else 0.0
      m.machine_record_date = new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(r(4).toString).getTime)

      if (r(3) != null) {
        val head = s"<head>${r(3)}</head>"
        val document = DocumentHelper.parseText(head)
        val root = document.getRootElement
        val it = root.elementIterator()
        while (it.hasNext) {
          val element = it.next()
          val ColName = element.attributeValue("ColName")
          if (!element.getTextTrim.equals("null") && element.getTextTrim.nonEmpty) {
            ColName match {
              case "主轴转速" => m.machine_record_mainshaft_speed = element.getTextTrim.toDouble
              case "主轴倍率" => m.machine_record_mainshaft_multiplerate = element.getTextTrim.toDouble
              case "主轴负载" => m.machine_record_mainshaft_load = element.getTextTrim.toDouble
              case "进给倍率" => m.machine_record_feed_speed = element.getTextTrim.toDouble
              case "进给速度" => m.machine_record_feed_multiplerate = element.getTextTrim.toDouble
              case "PMC程序号" => m.machine_record_pmc_code = element.getTextTrim.toDouble
              case "循环时间" => m.machine_record_circle_time = element.getTextTrim.toDouble
              case "运行时间" => m.machine_record_run_time = element.getTextTrim.toDouble
              case "有效轴数" => m.machine_record_effective_shaft = element.getTextTrim.toDouble
              case "总加工个数" => m.machine_record_amount_process = element.getTextTrim.toDouble
              case "已使用内存" => m.machine_record_use_memory = element.getTextTrim.toDouble
              case "未使用内存" => m.machine_record_free_memory = element.getTextTrim.toDouble
              case "可用程序量" => m.machine_record_amount_use_code = element.getTextTrim.toDouble
              case "注册程序量" => m.machine_record_amount_free_code = element.getTextTrim.toDouble
              case _ => ""
            }
          }

        }
      }

      m
    })

    val result = source
      .withColumn("etl_date", date_format(date_sub(current_date(), 1), "yyyyMMdd"))

    result.show()

    result.write.format("hive").partitionBy("etl_date").saveAsTable("dwd.fact_machine_learning_data")

  }
  case class machine(
                      var machine_record_id: Int = 0,
                      var machine_id: Double = 0.0,
                      var machine_record_state: Double = 0.0,
                      var machine_record_mainshaft_speed: Double = 0.0,
                      var machine_record_mainshaft_multiplerate: Double = 0.0,
                      var machine_record_mainshaft_load: Double = 0.0,
                      var machine_record_feed_speed: Double = 0.0,
                      var machine_record_feed_multiplerate: Double = 0.0,
                      var machine_record_pmc_code: Double = 0.0,
                      var machine_record_circle_time: Double = 0.0,
                      var machine_record_run_time: Double = 0.0,
                      var machine_record_effective_shaft: Double = 0.0,
                      var machine_record_amount_process: Double = 0.0,
                      var machine_record_use_memory: Double = 0.0,
                      var machine_record_free_memory: Double = 0.0,
                      var machine_record_amount_use_code: Double = 0.0,
                      var machine_record_amount_free_code: Double = 0.0,
                      var machine_record_date: Timestamp = null,
                      var dwd_insert_user: String = "user1",
                      var dwd_insert_time: Timestamp = new Timestamp(System.currentTimeMillis()),
                      var dwd_modify_user: String = "user1",
                      var dwd_modify_time: Timestamp = new Timestamp(System.currentTimeMillis())
                    )
}

