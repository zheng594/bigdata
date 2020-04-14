package com.zheng.spark;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * Created by zheng on 2020-04-07.
 */
public class SparkClient {
    public static void main(String[] args) throws IOException {
        SparkLauncher sparkLauncher = new SparkLauncher();
        SparkAppHandle sparkAppHandle = sparkLauncher
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setMainClass("org.apache.spark.examples.SparkPi")
                .setAppResource("/Users/zheng/spark/spark-3.0.0/examples/jars/spark-examples_2.12-3.0.0-preview2.jar")
                .startApplication();

        SparkAppHandle.State state = sparkAppHandle.getState();
        String applicationId = null;
        while (state != SparkAppHandle.State.RUNNING) {
            applicationId = sparkAppHandle.getAppId();
            if(applicationId != null){
                System.out.println("applicationId:"+sparkAppHandle.getAppId());
                break;
            }
        }
    }
}
