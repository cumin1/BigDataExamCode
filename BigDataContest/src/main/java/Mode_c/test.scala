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
      .master("local[*]").enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.legacy.avro.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

    import spark.implicits._

    val tables = Array("user_info", "sku_info", "base_province", "base_region", "order_info", "order_detail")

    tables.foreach(r =>{
      spark.sql(s"create table ods_ds_hudi.${r} using hudi location 'hdfs://bigdata1:9000/user/hive/warehouse/ods_ds_hudi.db/${r}'")
      spark.sql(s"msck repair table ods_ds_hudi.${r}")
      spark.sql(s"show partitions ods_ds_hudi.${r}").show()
    })

    val tables_2 = Array("dim_user_info", "dim_sku_info", "dim_province", "dim_region")

    tables_2.foreach(r =>{
      spark.sql(s"create table dwd_ds_hudi.${r} using hudi location 'hdfs://bigdata1:9000/user/hive/warehouse/dwd_ds_hudi.db/${r}'")
      spark.sql(s"msck repair table dwd_ds_hudi.${r}")
      spark.sql(s"show partitions dwd_ds_hudi.${r}").show()
    })

  }
}
