package Mode_c.Moxie_0528

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, dense_rank, lit}

object store_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("store_1")
      .master("local[*]").getOrCreate()

    val order_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info")
      .load()

    val order_detail = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_detail")
      .load()

    //    order_info.show()
    //    order_detail.show()

    val source = order_info.join(order_detail, order_info("id") === order_detail("order_id"))
      .select("user_id", "sku_id")
      .distinct()

    val frame = source
      .withColumn("user_id", dense_rank().over(Window.orderBy("user_id")) - 1)
      .withColumn("sku_id", dense_rank().over(Window.orderBy("sku_id")) - 1)


    println("-------user_id_mapping与sku_id_mapping数据前5条如下：-------")
    frame
      .orderBy(asc("user_id"),asc("sku_id"))
      .limit(5)
      .foreach(r =>{
        println(r(0) + ":" + r(1))
      })


    import spark.implicits._
    val ints = frame.select("sku_id").distinct().orderBy("sku_id").map(_(0).toString.toInt).collect()

    val res2 = frame
      .groupBy("user_id")
      .pivot("sku_id", ints)
      .agg(
        lit(1.0)
      )
      .na.fill(0.0)
      .orderBy("user_id")
      .withColumn("user_id", col("user_id").cast("double"))


    println("---------------第一行前5列结果展示为---------------")
    res2.show()
//    println(res2.select("user_id","0","1","2","3").limit(1).map(_(0).toString).collect().mkString(","))
  }
}
