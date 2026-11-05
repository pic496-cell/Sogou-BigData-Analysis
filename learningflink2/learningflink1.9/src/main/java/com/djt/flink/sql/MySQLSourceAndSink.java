package com.djt.flink.sql;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
/**
 * @author dajiangtai
 * @create 2020-02-05-19:33
 */
public class MySQLSourceAndSink {
    public static void main(String[] args) throws Exception {
        //第一步：获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldTypes =new TypeInformation[]{
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        };

        RowTypeInfo rowTypeInfo =new RowTypeInfo(fieldTypes);

        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost/djtdb_test")
                .setUsername("root")
                .setPassword("root")
                .setQuery("select tvid, tvname,allnumber,commentnumber,supportnumber  from tvcount")
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        DataSource<Row> input = env.createInput(jdbcInputFormat);

        //第二步：Create a TableEnvironment
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);


        //第三步：注册表
        tableEnv.registerDataSet("tvcount2", input, "tvid,tvname,allnumber,commentnumber,supportnumber");

        //tableEnv.sqlQuery("select * from tvcount2").printSchema();

        //第四步：查询
        Table query = tableEnv.sqlQuery("select * from tvcount2").where("commentnumber < 10000");

        DataSet result = tableEnv.toDataSet(query, Row.class);

        result.print();
        System.out.println(input.count());

        //将Row对象写到mysql
        result.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost/djtdb_test")
                .setUsername("root")
                .setPassword("root")
                .setQuery("insert into tvcount_copy (tvid,tvname,allnumber,commentnumber,supportnumber) values (?,?,?,?,?)")
                .finish());


        //触发执行
        env.execute("insert data to mysql");

    }
}
