package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/*
14、根据dwd层的数据，请计算每个省份累计订单量（订单信息表一条算一个记录），
然后根据每个省份订单量从高到低排列，将结果打印到控制台（使用spark中的show算子，同时需要显示列名），
将执行结果复制并粘贴至客户端桌面【Release\任务B提交结果.docx】中对应的任务序号下；

例如：可以考虑首先生成类似的临时表A：
province_name	Amount（订单量）
A省	10122
B省	301
C省	2333333

然后生成结果类似如下：其中C省销量最高，排在第一列，A省次之，以此类推。
C省	A省	B省
2333333	10122	301
提示：可用str_to_map函数减轻工作量
 */
object compute_15 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val order = spark.table("dwd.fact_order_info")
      .select("province_id","final_total_amount")

    val province = spark.table("dwd.dim_province")

    val source = order
      .join(province, order("province_id") === province("id"))
      .withColumnRenamed("name", "province_name")
      .select("province_name","final_total_amount")
      .groupBy("province_name")
      .agg(count("final_total_amount") as "Amount")
      .orderBy(desc("Amount"))

    val arr = source.collect()

    var tmp = spark.createDataFrame(Seq(
      ("tmp", "tmp")
    )).toDF("tmp1", "tmp2")

    for (data <- arr.sortBy(_.getLong(1)).reverse) {

      tmp = tmp.withColumn(data.getString(0), lit(data.getLong(1)))

    }

    val result = tmp.drop("tmp1","tmp2")
    result.show()

  }
}
