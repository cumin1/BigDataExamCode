package TaskBook4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import java.util.Properties

object B_compute {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书4 计算")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    // todo (1)
    // 统计每个月（change_start_time的月份）、每个设备、每种状态的时长，若某状态当前未结束（即change_end_time值为空）则该状态不参与计算。
    val result1 = spark.table("hudi_gy_dwd.fact_change_record")
      .filter(col("ChangeEndTime").isNotNull)
      .select(
        year(col("ChangeStartTime")) as "year",
        month(col("ChangeStartTime")) as "month",
        col("ChangeMachineID") as "machine_id",
        col("ChangeRecordState") as "change_record_state",
        (unix_timestamp(col("ChangeEndTime")) - unix_timestamp(col("ChangeStartTime"))) as "one_total_time"
      )
      .groupBy(
        "year","month","machine_id","change_record_state"
      )
      .agg(
        sum("one_total_time") as "duration_time"
      )
      .select(
        "machine_id","change_record_state","duration_time","year","month"
      )

    result1.createTempView("machine_state_time")
    spark.sql("select * from machine_state_time order by machine_id desc,duration_time desc limit 10").show()


    // todo (2)
    // 统计每个车间中所有设备运行时长（即设备状态为“运行”）的中位数在哪个设备（为偶数时，两条数据原样保留输出）
    val fact_change_record = spark.table("hudi_gy_dwd.fact_change_record")
    val dim_machine = spark.table("hudi_gy_dwd.dim_machine")

    val source1 = fact_change_record
      .join(dim_machine, fact_change_record("ChangeMachineID") === dim_machine("BaseMachineID"))
      .filter(fact_change_record("ChangeEndTime").isNotNull)
      .filter(fact_change_record("ChangeRecordState") === "运行")
      .select(
        fact_change_record("ChangeMachineID") as "machine_id",
        dim_machine("MachineFactory") as "machine_factory",
        (unix_timestamp(col("ChangeEndTime")) - unix_timestamp(col("ChangeStartTime"))) as "total_time"
      )
      .groupBy("machine_id","machine_factory")
      .agg(
        sum("total_time") as "total_running_time"
      )

    spark.udf.register("medain",(ids:Array[Int],times:Array[Int])=>{
      var r = ""
      if (times.length % 2 == 0){
        r = ids(times.length / 2) + "," + ids((times.length / 2) - 1)
      }else{
        r = ids(times.length / 2).toString
      }
      r
    })

    import spark.implicits._

    val medain_ds_list = source1
      .orderBy("total_running_time")
      .groupBy("machine_factory")
      .agg(
        collect_list("machine_id") as "ids",
        collect_list("total_running_time") as "times"
      )
      .withColumn("medain_id_list", expr("medain(ids,times)"))
      .select("medain_id_list")
      .map((_)(0).toString)
      .flatMap(_.split(","))
      .collect()

    println(medain_ds_list.mkString("Array(", ", ", ")"))

    val result2 = source1
      .filter(col("machine_id").isin(medain_ds_list: _*))
      .select("machine_id", "machine_factory", "total_running_time")

    result2.createTempView("machine_running_median")
    spark.sql("select * from machine_running_median order by machine_factory desc,machine_id desc limit 5").show()

     // todo (3)
     // 基于全量历史数据计算各设备生产一个产品的平均耗时
    val fact_produce_record = spark.table("hudi_gy_dwd.fact_produce_record")
    val source2 = fact_produce_record
      .filter(col("ProduceCodeEndTime") =!= "1900-01-01 00:00:00")
      .dropDuplicates("ProduceRecordID","ProduceMachineID")
      .select(
        col("ProduceRecordID") as "produce_record_id",
        col("ProduceMachineID") as "produce_machine_id",
        (unix_timestamp(col("ProduceCodeEndTime")) - unix_timestamp(col("ProduceCodeStartTime"))) as "producetime"
      )

    val source3 = source2
      .groupBy("produce_machine_id")
      .agg(
        avg("producetime") as "produce_per_avgtime"
      )
      .select(
        col("produce_machine_id") as "m_id",
        col("produce_per_avgtime") as "produce_per_avgtime"
      )

    val result3 = source2
      .join(source3, source2("produce_machine_id") === source3("m_id"))
      .filter(col("producetime") > col("produce_per_avgtime"))
      .withColumn("produce_per_avgtime",col("produce_per_avgtime").cast(DataTypes.IntegerType))
      .select("produce_record_id", "produce_machine_id", "producetime", "produce_per_avgtime")

    result3.createTempView("machine_produce_per_avgtime")
    spark.sql("select * from machine_produce_per_avgtime order by produce_machine_id desc limit 3").show()


    val clickhouse_url = "jdbc:clickhouse://bigdata2:8123/shtd_industry"
    val prop = new Properties()
    prop.put("driver","com.clickhouse.jdbc.ClickHouseDriver")
    prop.put("user","default")
    prop.put("password","123456")

    result1.write.mode("append").jdbc(clickhouse_url,"machine_state_time",prop)
    //create table machine_state_time(machine_id int,change_record_state String,duration_time int,year int,month int) engine = Memory;
    result2.write.mode("append").jdbc(clickhouse_url,"machine_running_median",prop)
    //create table machine_running_median(machine_id int,machine_factory int,total_running_time int) engine = Memory;
    result3.write.mode("append").jdbc(clickhouse_url,"machine_produce_per_avgtime",prop)
    //create table machine_produce_per_avgtime(produce_record_id int,produce_machine_id int,producetime int,produce_per_avgtime int) engine = Memory;

  }
}
