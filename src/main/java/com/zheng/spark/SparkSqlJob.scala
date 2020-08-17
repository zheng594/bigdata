package com.zheng.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import  org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver
/**
 * Created by zheng on 2020/8/14
 */
object SparkSqlJob {
    private var sparkSession : SparkSession = null

    def init()={
        if(sparkSession == null){
            sparkSession = SparkSession
                .builder.enableHiveSupport()
                .appName("SparkSql")
                .master("spark://zheng.local:7077")  //提交到集群
                .getOrCreate()
        }
    }

    def runJob(args: Array[String]): DataFrame = {
        this.init()
        sparkSession.sql(args(0))
    }
}
