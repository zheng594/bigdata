package com.zheng.spark;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * Created by zheng on 2020-04-07.
 */
public class SparkPythonClient {
    public static void main(String[] args) throws IOException {
        SparkLauncher sparkLauncher = new SparkLauncher();
        SparkAppHandle sparkAppHandle = sparkLauncher
                .setMaster("yarn")
                .setSparkHome("/Users/zheng/spark-3.0.0")
                .setDeployMode("cluster")
                .setConf("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/Users/zheng/project/bigdata/src/main/java/com/zheng/spark/test.py")
                .setConf("spark.driver.memory", "2g")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.cores", "3")
                .startApplication();

        SparkAppHandle.State state = sparkAppHandle.getState();
        String applicationId = null;
        while (state != SparkAppHandle.State.RUNNING) {
            System.out.println("applicationId:" + sparkAppHandle.getAppId() + "   ----------------------" + state.name());

            applicationId = sparkAppHandle.getAppId();
            if (applicationId != null) {
                System.out.println("applicationId:" + sparkAppHandle.getAppId());
                break;
            }
        }


    }
}
