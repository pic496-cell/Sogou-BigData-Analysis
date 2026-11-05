package com.djt.flink.news;

import java.io.Serializable;

public class GlobalConfig implements Serializable {
    /**
     * 数据库driver class
     */
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String DB_URL = "jdbc:mysql://192.168.20.123:3306/test";
    /**
     * 数据库user name
     */
    public static final String USER_MAME = "hive";
    /**
     * 数据库password
     */
    public static final String PASSWORD = "hive";

}
