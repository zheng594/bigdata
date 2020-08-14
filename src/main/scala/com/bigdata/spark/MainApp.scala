package com.bigdata.spark

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession


/**
 * Created by zheng on 2020-04-07.
 */
object MainApp {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_CONF_DIR", "/Users/zheng/hadoop/hadoop-3.2.1/config")

        val spark = SparkSession
          .builder
          .appName("Spark Demo")
//          .master("yarn")
//          .master("local")
//          .master("spark://zheng-2.local:7077")
          .getOrCreate()

        spark.sql("show databases")
    }

}
