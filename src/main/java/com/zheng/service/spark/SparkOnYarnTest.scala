package com.zheng.service.spark

import com.zheng.service.spark.TableRelationship.sparkSession
import org.apache.spark.sql.SparkSession

/**
 * Created by zheng on 2020/9/8
 */
object SparkOnYarnTest {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
            .builder.enableHiveSupport()
            .appName("SparkSql")
            .master("yarn")
            .config("spark.yarn.queue", "spark")
            .getOrCreate()
        val sc = sparkSession.sparkContext
        var rdd = sc.textFile("/Users/zheng/Downloads/csv_data/transfer_sample_data.csv")
        rdd.toDebugString
    }

}
