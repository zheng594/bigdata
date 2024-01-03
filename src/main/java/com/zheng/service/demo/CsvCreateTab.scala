package com.zheng.service.demo

import org.apache.spark.sql.SparkSession

import java.util

/**
 * 读取csv创建hive表
 */
class CsvCreateTab {
    private var sparkSession: SparkSession = null

    def init() = {
        if (sparkSession == null) {
            sparkSession = SparkSession.builder()
                .appName("TableRelationship")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()
        }
    }

    def main(args: Array[String]): Unit = {
        this.init()
        val filePath = "file:///Users/zheng/Downloads/bi_sales.csv"
        val optMap = new util.HashMap[String, String]()
        optMap.put("header", "true")
        val df = sparkSession.read.options(optMap).csv(filePath)
        df.createOrReplaceTempView("csv_tab")
        sparkSession.sql("create table bi_sales as select * from csv_tab")

        print(1)
    }
}
