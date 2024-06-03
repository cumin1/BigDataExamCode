package moni_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes

import java.util.Properties

object B_compute {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算")
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    // todo (1)
    val province = spark.table("dwd.dim_province").select("region_id","id","name")
    val region = spark.table("dwd.dim_region").select("region_name","id")
    val order = spark.table("dwd.fact_order_info").select("province_id","final_total_amount","create_time")

    val result1 = order.join(province, order("province_id") === province("id"))
      .join(region, province("region_id") === region("id"))
      .select(
        province("id") as "province_id",
        province("name") as "province_name",
        region("id") as "region_id",
        region("region_name") as "region_name",
        order("final_total_amount") as "amount",
        year(col("create_time")) as "year",
        month(col("create_time")) as "month"
      )
      .groupBy("province_id", "province_name", "region_id", "region_name", "year", "month")
      .agg(
        sum("amount") as "total_amount",
        count("amount") as "total_count"
      )
      .withColumn("sequence", row_number().over(Window.partitionBy("year", "month", "region_id").orderBy(desc("total_amount"))))
      .select( "province_id", "province_name", "region_id", "region_name", "total_amount", "total_count", "sequence", "year", "month")
      .withColumn("total_amount",col("total_amount").cast("double"))

    result1.createOrReplaceTempView("province_consumption_day_aggr")
    spark.sql(
      """
        |select province_id,province_name,region_id,region_name,cast(total_amount as bigint),total_count,sequence,year,month
        |from province_consumption_day_aggr
        |order by total_count desc,total_amount desc,province_id desc
        |limit 5
        |""".stripMargin).show()

//    result1.write.format("hive").mode("overwrite").saveAsTable("dws.province_consumption_day_aggr")


    // todo (2)
    val data = spark.table("dws.province_consumption_day_aggr")

    val source = data.filter(col("year") === 2020 && col("month") === 4)
      .drop("sequence", "year", "month")

    val frame1 = source.withColumn("provinceavgconsumption", col("total_amount") / col("total_count"))
      .select("province_id", "province_name", "region_id", "region_name", "provinceavgconsumption")

    val frame2 = source.groupBy("region_id")
      .agg(
        avg(col("total_amount")/col("total_count")) as "regionavgconsumption"
      )

    val result2 = frame1.join(frame2, Seq("region_id"))
      .withColumn("comparison",
        when(col("provinceavgconsumption") > col("regionavgconsumption"), "高")
          .when(col("provinceavgconsumption") < col("regionavgconsumption"), "低")
          .otherwise("相同")
      )
      .withColumnRenamed("province_id", "provinceid")
      .withColumnRenamed("province_name", "provincename")
      .withColumnRenamed("region_id", "regionid")
      .withColumnRenamed("region_name", "regionname")
      .select("provinceid", "provincename", "provinceavgconsumption", "regionid", "regionname", "regionavgconsumption", "comparison")

    result2.createOrReplaceTempView("provinceavgcmpregion")
    spark.sql(
      """
        |select * from provinceavgcmpregion
        |order by provinceid desc,provinceavgconsumption desc,regionavgconsumption desc
        |limit 5
        |""".stripMargin).show()

    // todo (3)
    val source1 = data.filter(col("year") === 2020)
      .drop("total_count", "year", "month")

    val result3 = source1
      .withColumn("total_amount",col("total_amount").cast("bigint"))
      .filter(col("sequence") <= 3)
      .withColumn("province_id_2", lead("province_id", 1).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("province_id_3", lead("province_id", 2).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("province_name_2", lead("province_name", 1).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("province_name_3", lead("province_name", 2).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("total_amount_2", lead("total_amount", 1).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .withColumn("total_amount_3", lead("total_amount", 2).over(Window.partitionBy("region_id").orderBy(desc("total_amount"))))
      .filter(col("sequence") === 1)
      .withColumn("provinceids", concat(col("province_id"), lit(","), col("province_id_2"), lit(","), col("province_id_3")))
      .withColumn("provincenames", concat(col("province_name"), lit(","), col("province_name_2"), lit(","), col("province_name_3")))
      .withColumn("provinceamount", concat(col("total_amount"), lit(","), col("total_amount_2"), lit(","), col("total_amount_3")))
      .withColumnRenamed("region_id", "regionid")
      .withColumnRenamed("region_name", "regionname")
      .select("regionid", "regionname", "provinceids", "provincenames", "provinceamount")


    result3.createOrReplaceTempView("regiontopthree")
    spark.sql(
      """
        |select * from regiontopthree
        |order by regionid asc
        |limit 5
        |""".stripMargin).show()

    val properties = new Properties() {
      {
        setProperty("driver", "com.mysql.jdbc.Driver")
        setProperty("user", "root")
        setProperty("password", "123456")
      }
    }
    val mysql_url = "jdbc:mysql://bigdata1:3306/shtd_result?useSSL=false&useUnicode=true&characterEncoding=utf8"
    result3.write.mode("append").jdbc(mysql_url,"regiontopthree",properties)
  }
}
