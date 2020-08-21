package com.zheng.service.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver
import org.apache.spark.sql.internal.SQLConf

/**
 * Created by zheng on 2020/8/14
 */
object SparkSqlJob {
    private var sparkSession: SparkSession = null
//    System.setProperty("HADOOP_CONF_DIR", "/Users/libinsong/Documents/codes/spark-examples/src/main/resources/hadoop")

    System.setProperty("HADOOP_USER_NAME", "zheng")

    def init() = {
        if (sparkSession == null) {
            val conf = new SQLConf()//会自动读取resource目录下的xml配置
            sparkSession = SparkSession
                .builder.enableHiveSupport()
                .appName("SparkSql")
                .master("spark://zheng.local:7077") //提交到集群
                .getOrCreate()
            val sc: SparkContext = sparkSession.sparkContext
            sc.setLogLevel("WARN")
        }
    }

    def runJob(sql: String): Array[Row] = {
        this.init()
        var df: Dataset[Row] = sparkSession.sql(sql)
        df.show(10)
        df.collect()
    }
}
