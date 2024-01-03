package com.zheng.service.demo

import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors

object SparkNewSessionDemo {
    val taskSubmitExecutor = Executors.newCachedThreadPool

    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
            .builder.enableHiveSupport()
            .appName("SparkSql")
            .master("yarn") //提交到集群
//            .master("spark://zheng.local:7077") //提交到集群
            .getOrCreate()

//        print("1------" + sparkSession.sparkContext.applicationId)
//        ConverterUtils.toApplicationId("application_1630402517125_0004")
//        print("2------" + sparkSession.sparkContext.applicationId)
//        val sparkSession2 = sparkSession.newSession()
//        print("3------" + sparkSession.sparkContext.applicationId)
        print()

        //        val threadpool = new ThreadPoolExecutor(
        //            3,
        //            8,
        //            500,
        //            TimeUnit.MILLISECONDS,
        //            new LinkedBlockingQueue[Runnable]()
        //        )
        //
        //        for(i <- 1 to 5){
        //            threadpool.execute(new Runnable {
        //                override def run(): Unit = {
        //                    val sparkSession2 = sparkSession.newSession()
        //                    sparkSession2.read.text("/difficulty.log")
        //                }
        //            })
        //        }

//        threadpool.shutdown()
    }
}
