package com.djt.flink.config;

import java.io.Serializable;

/**
 * 在生产上一般通过配置中心来管理
 */
public class GlobalConfig implements Serializable {
    /**
     * 数据库driver class
     */
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String DB_URL = "jdbc:mysql://localhost/djtdb_test";
    /**
     * 数据库user name
     */
    public static final String USER_MAME = "root";
    /**
     * 数据库password
     */
    public static final String PASSWORD = "root";

    //查询mysql SQL语句
    public static String QUERYSQL = "select * from dbus_flow";

    //插入mysql SQL 语句
    public static String INSERTSQL = "insert into  dbus_flow_copy (flowId,mode,databaseName,tableName,hbaseTable,family,commitBatch) VALUES (?,?,?,?,?,?,?) on DUPLICATE KEY UPDATE flowId = VALUES(flowId),mode = VALUES(mode),databaseName = VALUES(databaseName),tableName = VALUES(tableName),hbaseTable = VALUES(hbaseTable),family = VALUES(family),commitBatch = VALUES(commitBatch)";
    /**
     * 批量提交size
     */
    public static final int BATCH_SIZE = 2;

    //HBase相关配置
    public static final String HBASE_ZOOKEEPER_QUORUM = "hadoop3-1,hadoop3-2,hadoop3-3";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase";
    public static final String HBASE_TABLE_READER = "topic_offset";
    public static final String HBASE_TABLE_WRITER = "topic_offset_copy";

    public static final String HBASE_TABLE_COLUMNFAMILY = "offset";


}
