package com.zheng.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by zheng on 2020/4/9.
 */
public class HdfsClient {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://localhost:9000");

        //1获取文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        //2拷贝本地数据到集群
        fileSystem.copyFromLocalFile(new Path("/Users/zheng/difficulty.log"),new Path("/difficulty.log"));

        //3.关闭文件系统
        fileSystem.close();
    }
}
