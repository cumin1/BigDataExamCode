package Mode_c.Moxie_0530

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.dom4j.DocumentHelper

import java.sql
import java.sql.Timestamp
import java.text.SimpleDateFormat
object industry_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("moxie")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.storeAssignmentPolicy","LEGACY")
      .getOrCreate()

    val MachineData = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_industry?useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "MachineData")
      .load()

    MachineData.show(5)
    import spark.implicits._
    val res = MachineData.map(r => {
      val machine1 = new machine()
      machine1.machine_record_id = r(0).toString.toInt
      machine1.machine_id = r(1).toString.toDouble
      machine1.machine_record_state = if (r(2).equals("报警")) 1.0 else 0.0
      machine1.machine_record_date = new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(r(4).toString).getTime)

      if (r(3) != null) {
        val document = DocumentHelper.parseText(s"<rows>${r(3)}</rows>")
        val root = document.getRootElement
        val elemntInterator = root.elementIterator()

        while (elemntInterator.hasNext) {
          val element = elemntInterator.next()
          val colname = element.attributeValue("ColName")

          if (!element.getTextTrim.equals("null") && element.getTextTrim.nonEmpty) {
            colname match {
              case "主轴转速" => machine1.machine_record_mainshaft_speed = element.getTextTrim.toDouble
              case "主轴倍率" => machine1.machine_record_mainshaft_multiplerate = element.getTextTrim.toDouble
              case "主轴负载" => machine1.machine_record_mainshaft_load = element.getTextTrim.toDouble
              case "进给倍率" => machine1.machine_record_feed_speed = element.getTextTrim.toDouble
              case "进给速度" => machine1.machine_record_feed_multiplerate = element.getTextTrim.toDouble
              case "PMC程序号" => machine1.machine_record_pmc_code = element.getTextTrim.toDouble
              case "循环时间" => machine1.machine_record_circle_time = element.getTextTrim.toDouble
              case "运行时间" => machine1.machine_record_run_time = element.getTextTrim.toDouble
              case "有效轴数" => machine1.machine_record_effective_shaft = element.getTextTrim.toDouble
              case "总加工个数" => machine1.machine_record_amount_process = element.getTextTrim.toDouble
              case "已使用内存" => machine1.machine_record_use_memory = element.getTextTrim.toDouble
              case "未使用内存" => machine1.machine_record_free_memory = element.getTextTrim.toDouble
              case "可用程序量" => machine1.machine_record_amount_use_code = element.getTextTrim.toDouble
              case "注册程序量" => machine1.machine_record_amount_free_code = element.getTextTrim.toDouble
              case _ => ""
            }

          }

        }

      }
      machine1
    })

    res.orderBy("machine_record_id").show(5)



    spark.stop()
  }
  case class machine(var machine_record_id:Int = 1,
                     var machine_id:Double = 0.0,
                     var machine_record_state:Double = 0.0,
                     var machine_record_mainshaft_speed:Double = 0.0,
                     var machine_record_mainshaft_multiplerate:Double = 0.0,
                     var machine_record_mainshaft_load:Double = 0.0,
                     var machine_record_feed_speed:Double = 0.0,
                     var machine_record_feed_multiplerate:Double = 0.0,
                     var machine_record_pmc_code:Double = 0.0,
                     var machine_record_circle_time:Double = 0.0,
                     var machine_record_run_time:Double = 0.0,
                     var machine_record_effective_shaft:Double = 0.0,
                     var machine_record_amount_process:Double = 0.0,
                     var machine_record_use_memory:Double = 0.0,
                     var machine_record_free_memory:Double = 0.0,
                     var machine_record_amount_use_code:Double = 0.0,
                     var machine_record_amount_free_code:Double = 0.0,
                     var machine_record_date:Timestamp = null,
                     var dwd_insert_user:String = "user1",
                     var dwd_insert_time:Timestamp = new Timestamp(System.currentTimeMillis()),
                     var dwd_modify_user:String = "user1",
                     var dwd_modify_time:Timestamp = new Timestamp(System.currentTimeMillis()))
}
