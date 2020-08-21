package com.zheng.service;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * Created by zheng on 2020-04-07.
 */
public class SparkJarClient {
    public static void main(String[] args) throws IOException {
        SparkLauncher sparkLauncher = new SparkLauncher();
        SparkAppHandle sparkAppHandle = sparkLauncher
                .setMaster("yarn")
                .setSparkHome("/Users/zheng/spark-3.0.0")
                .setDeployMode("cluster")
                .setMainClass("org.apache.spark.examples.SparkPi")
                .setAppResource("/Users/zheng/spark-3.0.0/examples/jars/spark-examples_2.12-3.0.0-preview2.jar")
                .setConf("spark.driver.memory", "2g")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.cores", "3")
//                .setConf("spark.yarn.queue","spark")
                .startApplication();

        SparkAppHandle.State state = sparkAppHandle.getState();
        String applicationId = null;
        while (true) {
            System.out.println("applicationId:" + sparkAppHandle.getAppId() + "   ----------------------" + state.name());

            if(state == SparkAppHandle.State.RUNNING){
                break;
            }

            applicationId = sparkAppHandle.getAppId();
            if (applicationId != null) {
                System.out.println("applicationId:" + sparkAppHandle.getAppId());
                break;
            }
        }




    }
}
