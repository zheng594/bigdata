package com.zheng.service.yarn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkClient {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
            .builder.enableHiveSupport()
            .appName("SparkSql")
            .master("local")
            .getOrCreate()
        //        sparkSession.sql("select * from bi_sales").write.parquet("/parquet/bi_sales.parquet")
        val df = sparkSession.read.parquet("/parquet/*");
        df.cache()
        val df1 = df.groupBy(col("the_year"))
//        val df2 = df.map(_=>(_,null)).take(1)

        df.foreach(item=>print(item))
//        df.foreachPartition(item=>print(item))
//
//        df.filter("the_year=2017")
//        print()


//        val words = Array("one", "two", "two", "three", "three", "three")
//        val wordPairsRDD = sparkSession.sparkContext.parallelize(words).map(word => (word, 1))
//
//        val wordCountsWithReduce = wordPairsRDD
//            .reduceByKey(_ + _)
//            .collect()

    }

}
