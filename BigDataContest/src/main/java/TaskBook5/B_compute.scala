package TaskBook5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.DataTypes

import java.util.Properties

object B_compute {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书5 计算")
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

    // todo (1)
    val fact_change_record = spark.table("dwd.fact_change_record")
    val dim_machine = spark.table("dwd.dim_machine")

    val source1 = fact_change_record
      .select("changemachineid", "changestarttime", "changeendtime","changerecordstate")
    val source2 = dim_machine
      .select("basemachineid","machinefactory")

    val source3 = source1
      .join(source2, source1("changemachineid") === source2("basemachineid"))
      .filter(col("changerecordstate") === "运行")
      .filter(col("changeendtime").isNotNull)
      .withColumn("running_time", unix_timestamp(col("changeendtime")) - unix_timestamp(col("changestarttime")))
      .groupBy("basemachineid", "machinefactory")
      .agg(
        sum("running_time") as "total_running_time"
      )

    spark.udf.register("median",(ids:Array[Int],times:Array[Int])=>{
      var r = ""
      if (times.length % 2 == 0){
        r = ids(times.length / 2) + "," + ids((times.length / 2) - 1)
      }else{
        r = ids(times.length / 2).toString
      }
      r
    })

    import spark.implicits._
    val median_ids = source3
      .orderBy("total_running_time")
      .groupBy("machinefactory")
      .agg(
        collect_list("basemachineid") as "ids",
        collect_list("total_running_time") as "times"
      )
      .withColumn("median", expr("median(ids,times)"))
      .select("median")
      .map(_(0).toString)
      .flatMap(_.split(","))
      .collect()

    val result1 = source3
      .filter(col("basemachineid").isin(median_ids: _*))
      .withColumn("total_running_time",col("total_running_time").cast(DataTypes.IntegerType))
      .select(
        col("basemachineid") as "machine_id",
        col("machinefactory") as "machine_factory",
        col("total_running_time") as "total_running_time"
      )

    result1.createTempView("machine_running_median")
    spark.sql("select * from machine_running_median order by machine_factory desc,machine_id desc limit 5").show()

//    result1.write.jdbc(url,"machine_running_median",prop)

    // todo (2)
    val source4 = fact_change_record
      .select("changemachineid", "changestarttime", "changeendtime","changerecordstate")
    val source5 = dim_machine
      .select("basemachineid","machinefactory")

    val source6 = source4
      .join(source5, source1("changemachineid") === source2("basemachineid"))
      .filter(col("changerecordstate") === "运行")
      .filter(col("changeendtime").isNotNull)
      .withColumn("running_time", unix_timestamp(col("changeendtime")) - unix_timestamp(col("changestarttime")))
      .withColumn("start_month", date_format(col("changestarttime"), "yyyy-MM"))
      .select(
        "start_month", "machinefactory", "running_time", "changemachineid"
      )

    // 计算各车间的平均运行时长
    val factory_avg = source6
      .groupBy(
        "start_month", "machinefactory",
      )
      .agg(
        avg("running_time") as "factory_avg"
      )
      .withColumnRenamed("start_month","sm")
      .withColumnRenamed("machinefactory","mf")

    val result2 = source6
      .groupBy(
        "start_month", "changemachineid", "machinefactory"
      )
      .agg(
        avg("running_time") as "company_avg"
      )
      .join(factory_avg,
        source6("start_month") === factory_avg("sm") && source6("machinefactory") === factory_avg("mf")
      )
      .select(
        "start_month", "machinefactory", "factory_avg", "company_avg"
      )
      .withColumn("comparison",
        when(col("factory_avg") > col("company_avg"), "高")
          .when(col("factory_avg") < col("company_avg"), "低")
          .otherwise("相同")
      )
      .withColumnRenamed("machinefactory", "machine_factory")
      .select(
        "start_month", "machine_factory", "comparison", "factory_avg", "company_avg"
      )

    result2.createTempView("machine_running_compare")
    spark.sql("select * from machine_running_compare order by machine_factory desc limit 2").show()

//    result2.write.jdbc(url,"machine_running_compare",prop)


    // todo (3)
    val result3 = fact_change_record
      .select("changemachineid", "changerecordstate", "changestarttime", "changeendtime")
      .withColumn("row",row_number().over(Window.partitionBy("changemachineid").orderBy(desc("changestarttime"))))
      .filter(col("row") <= 2)
      .drop("row")
      .withColumn("row",row_number().over(Window.partitionBy("changemachineid").orderBy(asc("changestarttime"))))
      .filter(col("row") === 1)
      .select(
        col("changemachineid") as "machine_id",
        col("changerecordstate") as "record_state",
        col("changestarttime") as "change_start_time",
        col("changeendtime") as "change_end_time"
      )

    result3.createTempView("recent_state")
    spark.sql("select * from recent_state order by machine_id desc limit 5").show()

//    result3.write.jdbc(url,"recent_state",prop)

  }
}
