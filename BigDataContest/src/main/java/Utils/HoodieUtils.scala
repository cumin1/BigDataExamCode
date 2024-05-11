package Utils

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HoodieUtils {

  def read(spark:SparkSession,filePath:String): DataFrame ={
    spark.read.format("hudi").load(filePath)
  }

  def write(data:DataFrame,partitionPath:String,recordkey:String,precombine:String,table:String,database:String,savePath:String,saveMode: SaveMode){
    data.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PARTITIONPATH_FIELD.key(), partitionPath)
      .option(RECORDKEY_FIELD.key(), recordkey)
      .option(PRECOMBINE_FIELD.key(), precombine)
      .option("hoodie.table.name", table)
      .option("hoodie.datasource.write.allowNullValue", "true") // 允许写入空值
      .option("hoodie.datasource.write.hive_style_partitioning","true")
//      .option("hoodie.datasource.hive_sync.enable","true")
//      .option("hoodie.datasource.hive_sync.mode","hms")
//      .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://bigdata1:9083")
//      .option("hoodie.datasource.hive_sync.database", database)
//      .option("hoodie.datasource.hive_sync.table", table)
//      .option("hoodie.datasource.hive_sync.partition_fields", partitionPath)
//      .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .mode(saveMode)
      .save(savePath)

    println(database + "." + table + "抽取完成！！！！")

  }

}
