package com.zheng.service.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * @Author: zheng
 * @Date: 2021/11/1
 */
object CsvToParquet {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
            .builder.enableHiveSupport()
            .appName("SparkSql")
            .master("local")
            .getOrCreate()
        val sc = sparkSession.sparkContext
        val schema = StructType(
            Array(
                StructField("C_S_ID", StringType, true),
                StructField("C_S_ACCOUNT", StringType, true),
                StructField("C_E_TESTENUM", StringType, true),
                StructField("S_S_BIZID", StringType, true),
                StructField("S_D_EVENTOCCURTIME", LongType, true),
                StructField("C_S_ZFX1", StringType, true),
                StructField("ds", StringType, true),
                StructField("ts", StringType, true)
            )
        )

        val df = sparkSession.read.format("com.databricks.spark.csv")
            .schema(schema).option("delimiter", ",")
            .load("file:///Users/zheng/Downloads/parquet文件数据.csv")
            .write.parquet("file:///Users/zheng/Downloads/parquet_table_1");

        //        var rdd = sparkSession.read.csv("file:///Users/zheng/Downloads/MetricTest.csv")
        //            .write.parquet("file:///Users/zheng/Downloads/offline_metric.parquet");

    }
}
