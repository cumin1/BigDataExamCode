package Mode_c

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("电商1 特征工程")
      .master("local[*]").getOrCreate()

    import spark.implicits._

    // todo 1
    val order_info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info")
      .load()

    val order_detail = spark.read.format("jdbc")
      .option("url","jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","order_detail")
      .load()

    val sku_info = spark.read.format("jdbc")
      .option("url","jdbc:mysql://bigdata1:3306/shtd_store?useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","sku_info")
      .load()

    order_detail.show()
    sku_info.show()

    val all_user_sku = order_detail.join(order_info,order_info("id") === order_detail("order_id"))
      .select(order_info("user_id"),order_detail("sku_id"))
  }
}
