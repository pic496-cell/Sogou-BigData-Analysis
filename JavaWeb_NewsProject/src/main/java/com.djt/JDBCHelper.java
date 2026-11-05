package com.djt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JDBCHelper {
    // 获取数据库连接
    public static Connection getConnection() throws Exception {
        Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));
        return DriverManager.getConnection(
                ConfigurationManager.getProperty(Constants.JDBC_URL),
                ConfigurationManager.getProperty(Constants.JDBC_USER),
                ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
        );
    }

    // 执行查询并返回结果集
    public static ResultSet executeQuery(String sql) throws Exception {
        Connection conn = getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql);
        return stmt.executeQuery();
    }
}