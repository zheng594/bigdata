package com.zheng.spark

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Created by zheng on 2020/4/14.
 */
object SparkSqlYarnClient {
    private var spark : SparkSession = null

    def init()={
        if(spark == null){
            spark = SparkSession
                    .builder.enableHiveSupport()
                    .appName("SparkSql")
                    //                .master("local")  //本地模式
                    .master("spark://zheng-2.local:7077")  //提交到集群
                    .config("hive.metastore.warehouse.dir", "/Users/zheng/hadoop/warehouse")
                    .getOrCreate()
        }
    }

    def run(sql:String) :Array[Row]={
        this.init()
        spark.sql(sql).collect()
    }

    def main(args: Array[String]): Unit = {
        SparkSqlClient.run("show databases")
    }
}
