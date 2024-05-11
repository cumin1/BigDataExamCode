package stiei

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object load_csv {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("导入测试用数据")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://219.228.173.114:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val credit_data = spark.read.format("csv").option("header", "true").option("encoding", "gbk") //utf-8
      .load("hdfs://bigdata1:9000/stiei/credit_data.csv")

    val home_insurance = spark.read.format("csv").option("header", "true").option("encoding", "gbk")
      .load("hdfs://bigdata1:9000/stiei/home_insurance.csv")

    val insurance = spark.read.format("csv").option("header", "true").option("encoding", "gbk")
      .load("hdfs://bigdata1:9000/stiei/insurance.csv")

    val movies_ratings = spark.read.format("csv").option("header", "true").option("encoding", "gbk")
      .load("hdfs://bigdata1:9000/stiei/movies-ratings.csv")

    val output_profit = spark.read.format("csv").option("header", "true").option("encoding", "gbk")
      .load("hdfs://bigdata1:9000/stiei/output_profit.csv")

    val winequality_red = spark.read.format("csv").option("header", "true").option("encoding", "gbk")
      .load("hdfs://bigdata1:9000/stiei/winequality-red.csv")


    credit_data.show()
    home_insurance.show()
    insurance.show()
    movies_ratings.show()
    output_profit.show()
    winequality_red.show()

    credit_data.write.format("hive").mode("overwrite").saveAsTable("stiei_rgzn.credit_data")
    credit_data.write.format("hive").mode("overwrite").saveAsTable("stiei_rgzn.home_insurance")
    credit_data.write.format("hive").mode("overwrite").saveAsTable("stiei_rgzn.insurance")
    credit_data.write.format("hive").mode("overwrite").saveAsTable("stiei_rgzn.movies_ratings")
    credit_data.write.format("hive").mode("overwrite").saveAsTable("stiei_rgzn.output_profit")
    credit_data.write.format("hive").mode("overwrite").saveAsTable("stiei_rgzn.winequality_red")


    spark.stop()
  }
}
