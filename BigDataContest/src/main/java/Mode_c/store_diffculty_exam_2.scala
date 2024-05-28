package Mode_c

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object store_diffculty_exam_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("计算")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable","order_info")
      .load()
      .select("id","final_total_amount")

    import spark.implicits._

    val source = df
      .withColumn("diff_value", sum("final_total_amount").over(Window.orderBy(asc("id"))))
      .withColumnRenamed("id", "order_id")
      .orderBy("order_id")
      .select("order_id", "diff_value")

    val result = source
      .withColumn("diff", abs(col("diff_value") - lit(2023060600L)))
      .orderBy("diff")
      .limit(10)
      .select("order_id","diff_value")

    result.show()


  }
}
