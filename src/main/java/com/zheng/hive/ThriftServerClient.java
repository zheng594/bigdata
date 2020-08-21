package com.zheng.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.*;

/**
 * 通过jdbc的方式连接hiveserver2或spark thriftserver
 * 启动hiveserver2服务，/hive-3.1.2/bin/hiveserver2
 * 启动spark thriftserver服务，sh /spark-3.0.0/bin/start-thriftserver.sh
 * Created by zheng on 2020/4/9.
 */
@Service
public class ThriftServerClient {
    private static final Logger logger = LoggerFactory.getLogger(ThriftServerClient.class);

    private Connection getCon() {
        Connection conn = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "zheng", "");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    public ResultSet querySql(String sql) {
        ResultSet resultSet = null;
        Connection conn = this.getCon();
        if (conn == null) {
            return null;
        }
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(sql);

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                stmt.close();
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return resultSet;
    }

    public static void main(String[] args) {
        ThriftServerClient hiveClient = new ThriftServerClient();
        ResultSet resultSet = hiveClient.querySql("show databases");
        System.out.println(resultSet);
    }


}
