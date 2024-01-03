package com.zheng.service.demo

import org.apache.commons.lang3.StringUtils
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession

object Test {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder()
            .appName("TableRelationship")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()
        sparkSession.sparkContext.addSparkListener(new ServerListener())

        val sql =
//            "create table sales_child7 as " +
//            "insert into table sales_child7  " +
                "select concat(gender_a,'_',num) outside,* from " +
                "(select concat(gender,'_',time) gender_a ,* from " +
                "(select concat(the_year,month_of_year) time,store_sales+store_cost num,* from bi_sales))"
        val df = sparkSession.sql(sql)
        val df2 = df.createTempView("tdl")
        print(1)
    }
}
