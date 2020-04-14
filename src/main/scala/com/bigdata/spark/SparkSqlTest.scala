package com.bigdata.spark

import org.apache.spark.sql.SparkSession

/**
 * Created by zheng on 2020/4/14.
 */
object SparkSqlTest {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder.enableHiveSupport()
                .appName("SparkSql")
//                .master("local")  //本地模式
                .master("spark://zheng-2.local:7077")  //提交到集群
                .config("hive.metastore.warehouse.dir", "/Users/zheng/hive/warehouse")
                .getOrCreate()
        spark.sql("show databases").show()
    }
}
