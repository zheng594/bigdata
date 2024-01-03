package com.zheng.service.demo

import org.apache.spark.sql.SparkSession

object TransformationDemo {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
            .builder.enableHiveSupport()
            .appName("SparkSql")
//            .master("spark://zheng.local:7077")  //提交到集群
            .master("local[*]")
            .getOrCreate()
        import sparkSession.implicits._

        sparkSession.sparkContext.addSparkListener(new ServerListener())
        val sc = sparkSession.sparkContext
        val r1=sc.parallelize(Array((1,"spark"),(2,"tachyon"),(3,"hadoop")));
        val r2=sc.parallelize(Array((1,100),(2,90),(3,80)));
        val result = r1.join(r2);
        r1.map(_._1)
        r1.groupByKey(2)

        print()

//        val df = sparkSession.read.parquet("/parquet/*")
//        df.groupByKey(_=>_.0)
//        val df2= df.groupBy("the_year");
////        df.map(x=>{
////            (x(0),x(1))
////        }).foreach(row=>print(row))
//        print()
//        df.filter(row=>row.getString(1)=="2018").foreach(print(_))
//        print(df.take(1))
    }

}
