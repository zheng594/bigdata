package com.zheng.service.demo

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

import java.util

/**
 * 修改字段
 * 字段增加udf函数
 */
object ModifyColumn {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
            .appName("wordCount")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        val filePath = "file:///Users/zheng/千万倾斜数据.csv"
        val optMap = new util.HashMap[String, String]()
        optMap.put("header", "true")
        var df = sparkSession.read.options(optMap).csv(filePath)

        val fun: String => String = DigestUtils.md5Hex(_)
        val md5Udf = udf(fun)

        //方法1、select给字段加md5Udf函数
        val tmp = df.columns.map(c => {
            md5Udf(col(c)).alias(c)
        })
        val df1 = df.select(tmp: _*)
        df1.explain(true)
        df1.take(50).foreach(println(_))

        //方法2、withColumn给字段加md5Udf函数，当字段比较多时，性能会很差
        df.columns.foreach(c => {
            df = df.withColumn(c, md5Udf(col(c)))
        })
        df.take(50).foreach(println(_))
    }
}
