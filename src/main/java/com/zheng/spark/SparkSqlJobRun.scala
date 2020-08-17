package com.zheng.spark

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Created by zheng on 2020/4/14.
 */
object SparkSqlJobRun {

    def main(args: Array[String]): Unit = {
        var df = SparkSqlJob.runJob(Array("show databases"));
        print()
    }
}
