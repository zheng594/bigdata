package com.zheng.hive;

import java.sql.*;

/**
 * Created by zheng on 2020/4/9.
 */
public class HiveClient {
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "zheng", "");
        Statement stmt = con.createStatement();
        ResultSet resultSet = stmt.executeQuery("show databases" );
        System.out.println(resultSet.toString());
    }
}
