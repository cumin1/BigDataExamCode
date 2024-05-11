package shtd_store

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import java.util.Properties

object compute_3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("指标计算3")
      .master("local[*]").enableHiveSupport()
      .config("hive.exec.partition.mode", "nonstrict")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .getOrCreate()

    val fact_order_info = spark.table("dwd.fact_order_info")

    val source = fact_order_info.select("province", "city", "order_money")

    val df1 = source.groupBy("province").agg(avg("order_money") as "provinceavgconsumption")

    val df2 = source.withColumnRenamed("province", "provincename").withColumnRenamed("city", "cityname")
      .groupBy("provincename", "cityname").agg(avg("order_money") as "cityavgconsumption")

    val result = df1.join(df2, df1("province") === df2("provincename"))
      .withColumn("cityavgconsumption", col("cityavgconsumption").cast(DataTypes.DoubleType))
      .withColumn("provinceavgconsumption", col("provinceavgconsumption").cast(DataTypes.DoubleType))
      .withColumn("comparison", when(col("cityavgconsumption") > col("provinceavgconsumption"), "高")
        .when(col("cityavgconsumption") < col("provinceavgconsumption"), "低").otherwise("相同")
      )
      .select("cityname", "cityavgconsumption", "provincename", "provinceavgconsumption", "comparison")

    result.show()

    val clickhouse_prop = new Properties() {
      {
        setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        setProperty("user", "default")
        setProperty("password", "123456")
      }
    }
    val clickhouse_url = "jdbc:clickhouse://bigdata1:8123/shtd_result"

    result.write.mode("append").jdbc(clickhouse_url,"citymidcmpprovince",clickhouse_prop)

  }
}
