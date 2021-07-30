package com.zheng.service.demo

import org.apache.spark.sql.SparkSession

/**
 * 词频统计
 */
object WordCount {
    def main(args: Array[String]): Unit = {
        val sc = SparkSession.builder()
            .appName("wordCount")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        val filePath = "file:///Users/zheng/千万倾斜数据.csv"
        val file = sc.sparkContext.textFile(filePath)
        file.flatMap(_.split(",")).map((_, 1))
            .sample(false,0.5)
            .countByKey()
            .foreach(println(_))

//        val df = file.flatMap(_.split(","))
//            .map((_, 1))
//            .reduceByKey(_ + _)
//            .collect()


    }

}
