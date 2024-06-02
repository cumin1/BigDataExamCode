package model_b_train.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/*
请根据dwd_ds_hudi层的相关表，计算2020年销售量前10的商品，销售额前10的商品，
存入ClickHouse数据库shtd_result的topten表中（表结构如下），
然后在Linux的ClickHouse命令行中根据排名升序排序，查询出前5条

字段	类型	中文含义	备注
topquantityid	int	商品id	销售量前10的商品
topquantityname	text	商品名称	销售量前10的商品
topquantity	int	该商品销售量	销售量前10的商品
toppriceid	text	商品id	销售额前10的商品
toppricename	text	商品名称	销售额前10的商品
topprice	decimal	该商品销售额	销售额前10的商品
sequence	int	排名	所属排名
 */

object compute_11 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算训练")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val source = spark.read.format("hudi").load("hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
      .select("sku_id","sku_name","sku_num","order_price")
      .withColumn("sku_num",col("sku_num").cast("int"))


    val frame1 = source
      .groupBy("sku_id", "sku_name")
      .agg(
        count("sku_num") as "topquantity"
      )
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("topquantity"))))
      .withColumnRenamed("sku_id", "topquantityid")
      .withColumnRenamed("sku_name", "topquantityname")
      .orderBy("sequence")
      .limit(10)
      .select("topquantityid", "topquantityname", "topquantity","sequence")

    val frame2 = source
      .groupBy("sku_id", "sku_name")
      .agg(
        sum(col("order_price") * col("sku_num")) as "topprice"
      )
      .withColumn("tmp", lit("tmp"))
      .withColumn("sequence", row_number().over(Window.partitionBy("tmp").orderBy(desc("topprice"))))
      .withColumnRenamed("sku_id", "toppriceid")
      .withColumnRenamed("sku_name", "toppricename")
      .orderBy("sequence")
      .limit(10)
      .select("toppriceid", "toppricename", "topprice","sequence")

    val result = frame1.join(frame2, Seq("sequence"))
//      .withColumn("topprice",col("topprice").cast(DataTypes.StringType))
      .select("topquantityid", "topquantityname", "topquantity", "toppriceid", "toppricename", "topprice", "sequence")

    result.createOrReplaceTempView("topten")
    spark.sql(
      """
        |select * from topten
        |order by sequence asc
        |limit 5
        |""".stripMargin).show()

  }
}
