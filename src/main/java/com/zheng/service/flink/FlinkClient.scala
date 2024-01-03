//package com.zheng.service.flink
//
//import org.apache.flink.api.java.ExecutionEnvironment
//
//object FlinkClient {
//    def main(args: Array[String]): Unit = {
//        val env = ExecutionEnvironment.getExecutionEnvironment
//
//        // get input data
//        val text = env.readTextFile("/path/to/file")
//
//        val counts = text.flatMap { _.toLowerCase.split("\\s+") filter { _.nonEmpty } }
//            .map ( (_, 1) )
//            .groupBy(0)
////            .sum(1)
//
//        counts.writeAsCsv(outputPath, "\n", " ")
//    }
//
//}
