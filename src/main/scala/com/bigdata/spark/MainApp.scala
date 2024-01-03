package com.bigdata.spark

import org.apache.spark.sql.SparkSession


/**
 * Created by zheng on 2020-04-07.
 */
object MainApp {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder.enableHiveSupport()
          .appName("SparkSql")
//          .master("spark://zheng.local:7077")  //提交到集群
          .master("local")  //提交到集群
          .config("hive.metastore.warehouse.dir", "/Users/zheng/hadoop/warehouse")
          .getOrCreate()

        val df = spark.sql("select (select * from bi_sales) from bi_sales limit 1").collect()
        print(df)
    }

}
