package com.zheng.service.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator {
    def main(args: Array[String]): Unit = {
        val sc = SparkSession.builder()
            .appName("SparkOperator")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()
            .sparkContext

        val arrRDD =sc.parallelize(Array("a_1","b_2","c_3"))
        arrRDD.foreach(println)

//        arrRDD.mapPartitions(println(_))

        arrRDD.map(string=>{
            string.split("_")
        }).foreach(x=>{
            println(x.mkString(","))
        })

        arrRDD.flatMap(string=>{
            string.split("_")
        }).foreach(x=>{
            println(x.mkString(","))
        })


    }

}
