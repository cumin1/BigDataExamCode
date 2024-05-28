package Mode_c

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession,functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/*
5、根据MySQL的shtd_store.order_info表。按照id进行升序排序，取id小于等于20000的订单，
求出连续订单中平均金额数最大前1000000条的连续订单序列，若有多个平均金额数相同的连续订单序列，
要求输出连续订单数最少的订单序列，若多个平均金额数相同的连续订单序列中的订单个数同样相同，
则根据订单表主键集合进行字典降序排序。（要求订单连续序列的连续订单数大于或等于200条以上，即订单序列长度最少为200），
将计算结果存入MySQL数据库shtd_result的max_avg_order_price_seq表中（表结构如下），
然后在Linux的MySQL的命令行中查询shtd_result.max_avg_order_price_seq中seq_index为1、10、100、1000、1000000的数据，
将SQL语句复制粘贴至客户端桌面【Release\任务B提交结果.docx】中对应的任务序号下，将执行结果截图粘贴至客户端桌面【Release\任务B提交结果.docx】中对应的任务序号下。

字段	          类型	  中文含义	      备注
seq_index	      int	    序列排名	      mysql自然增长序列，不重复
avg_order_price	double	订单均价	      连续订单序列中的订单价格均值
id_range	      text	  订单表主键集合	订单表id（只存订单连续序列中订单id的最小值与订单id最大值）,用“_”拼接，如下所示：
                                                                                             37901_38720
matchnum	      Int   	序列长度

 */
object store_diffculty_exam_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("计算")
      .master("local[*]").enableHiveSupport()
      .config("hive.metastore.uris", "thrift://bigdata1:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test1?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable","order_info")
      .load()
      .select("id","final_total_amount")

    import spark.implicits._

    // 2. 数据处理
    val windowSpec = Window.orderBy("id")
    val dfWithIdLag = df.withColumn("prev_id", lag("id", 1).over(windowSpec))
//    dfWithIdLag.show(5)
    val dfWithDiff = dfWithIdLag.withColumn("diff", col("id") - col("prev_id").cast("int"))
//    dfWithDiff.show(5)
    val dfWithGroupId = dfWithDiff


    dfWithGroupId.show(5)

    val filteredDF = dfWithGroupId.filter($"id" <= 20000)
    val groupedDF = filteredDF.groupBy("diff").agg(
      min("id").alias("min_id"),
      max("id").alias("max_id"),
      avg("final_total_amount").alias("avg_order_price"),
      count("*").alias("matchnum")
    ).filter($"matchnum" >= 200)

    // 排序和取前1000000条
    val sortedDF = groupedDF.orderBy($"avg_order_price".desc, $"matchnum".asc, $"min_id".desc)
      .limit(1000000)

    sortedDF.show()



  }
}
