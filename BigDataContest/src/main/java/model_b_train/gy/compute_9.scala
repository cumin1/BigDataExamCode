package model_b_train.gy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object compute_9 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris","thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .getOrCreate()

    val fact_record = spark.table("dwd.fact_change_record")
    val dim_machine = spark.table("dwd.dim_machine")

    val source = fact_record
      .join(dim_machine, fact_record("ChangeMachineID") === dim_machine("BaseMachineID"))
      .filter(col("changerecordstate") === "运行")
      .filter(col("changeendtime").isNotNull)
      .select("ChangeMachineID", "MachineFactory", "changestarttime", "changeendtime")
      .withColumn("time",unix_timestamp(col("changeendtime")) - unix_timestamp(col("changestarttime")))
      .groupBy("ChangeMachineID","MachineFactory")
      .agg(sum("time") as "total_time")

    import spark.implicits._
    spark.udf.register("get_median",(ids:Array[Int],times:Array[Int])=>{
      var r = "";

      if (times.length % 2 == 0){
        r = ids((times.length / 2) - 1) + "," + ids(times.length / 2)
      }else{
        r = ids(times.length / 2).toString
      }
      r
    })

    val medain_ids = source
      .orderBy("total_time")
      .groupBy("MachineFactory")
      .agg(
        collect_list(col("ChangeMachineID")) as "ids",
        collect_list(col("total_time")) as "times",
      )
      .withColumn("medain_ids", expr("get_median(ids,times)"))
      .select("medain_ids")
      .map(_(0).toString)
      .flatMap(_.split(","))
      .collect()

    val result = source
      .filter(col("ChangeMachineID").isin(medain_ids: _*))
      .withColumnRenamed("ChangeMachineID", "machine_id")
      .withColumnRenamed("MachineFactory", "machine_factory")
      .withColumnRenamed("total_time", "total_running_time")
      .select("machine_id", "machine_factory", "total_running_time")

    result.createOrReplaceTempView("machine_running_median")
    spark.sql(
      """
        |select * from machine_running_median
        |order by machine_factory desc,machine_id desc
        |limit 5
        |""".stripMargin).show()


  }
}
