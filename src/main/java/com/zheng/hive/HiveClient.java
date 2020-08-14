package com.zheng.hive;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 启动hiveserver2服务，/hive-3.1.2/bin/hiveserver2
 * Created by zheng on 2020/4/9.
 */
@Service
public class HiveClient implements InitializingBean {
    private Connection con;

    @Override
    public void afterPropertiesSet() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "zheng", "");
    }

    public ResultSet querySql(String sql) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "zheng", "");
        ResultSet resultSet = null;
        if (con == null) {
            throw new Exception("连接异常");
        }
        Statement stmt = con.createStatement();
        resultSet = stmt.executeQuery(sql);
        return resultSet;
    }

    public static void main(String[] args) throws Exception {
        HiveClient hiveClient = new HiveClient();
        ResultSet resultSet = hiveClient.querySql("show databases");
        System.out.println(resultSet);
    }


}
