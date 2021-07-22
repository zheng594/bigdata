package com.zheng.service.demo

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by zheng on 2020/8/31
 */
object TableRelationship {
    private var sparkSession: SparkSession = null
    System.setProperty("HADOOP_USER_NAME", "zheng")

    def init() = {
        if (sparkSession == null) {
            val conf = new SQLConf() //会自动读取resource目录下的xml配置
            sparkSession = SparkSession
                .builder.enableHiveSupport()
                .appName("SparkSql")
                .master("spark://zheng.local:7077") //提交到集群
                .getOrCreate()
            val sc: SparkContext = sparkSession.sparkContext
            sc.setLogLevel("WARN")
        }
    }

    def main(args: Array[String]): Unit = {
        this.init()
        var df: DataFrame = sparkSession.table("sales_csv")
        df = df.selectExpr("the_year", "month_of_year").join(df.selectExpr("the_year"))
        print()
    }

}
