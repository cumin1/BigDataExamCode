package TaskBook2.compute_total

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import java.util.UUID

/*
2、根据dwd_ds_hudi层表统计每人每天下单的数量和下单的总金额，存入Hudi的dws_ds_hudi层的user_consumption_day_aggr表中（表结构如下），
然后使用spark -shell按照客户主键、订单总金额均为降序排序，查询出前5条，
将SQL语句复制粘贴至客户端桌面【Release\任务B提交结果.docx】中对应的任务序号下，
将执行结果截图粘贴至客户端桌面【Release\任务B提交结果.docx】中对应的任务序号下；
字段	类型	中文含义	备注
uuid	string	随机字符	随机字符，保证不同即可，作为primaryKey
user_id	int	客户主键
user_name	string	客户名称
total_amount	double	订单总金额	当天订单总金额。
total_count	int	订单总数	当天订单总数。同时可作为preCombineField（作为合并字段时，无意义，因为主键为随机生成）
year	int	年	订单产生的年,为动态分区字段
month	int	月	订单产生的月,为动态分区字段
day	int	日	订单产生的日,为动态分区字段
 */

object compute1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("任务书2 计算1")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.parquet.enableVectorizedReader","false")
      .getOrCreate()

    val order_info = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .select("final_total_amount", "user_id", "create_time")
      .withColumn("create_time",from_unixtime(unix_timestamp(col("create_time"),"yyyyMMdd"),"yyyy-MM-dd HH:mm:ss"))

    order_info.show(5)

    val user_info = spark.read.format("hudi").load("/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
      .withColumn("id",col("id").cast("bigint"))
      .select("id", "name")


    val uuidfuc = spark.udf.register("uuid",()=>{
      var uuid = UUID.randomUUID().toString.replace("-", "")
      uuid
    })


    val result :DataFrame = order_info
      .join(user_info, order_info("user_id") === user_info("id"))
      .select(
        order_info("user_id") as "user_id",
        user_info("name") as "user_name",
        order_info("final_total_amount") as "amount",
        year(order_info("create_time")) as "year",
        month(order_info("create_time")) as "month",
        dayofmonth(order_info("create_time")) as "day"
      ).groupBy(
        "user_id", "user_name", "year", "month", "day"
      ).agg(
        sum(col("amount")) as "total_amount",
        count(col("amount")) as "total_count"
      )
      .withColumn("uuid", uuidfuc())
      .select(
        "uuid", "user_id", "user_name", "total_amount", "total_count", "year", "month", "day"
      )

    result.show(5)
    result.createTempView("user_consumption_day_aggr")
    spark.sql("select * from user_consumption_day_aggr order by user_id desc,total_amount desc limit 5").show()

    spark.stop()
  }
}
