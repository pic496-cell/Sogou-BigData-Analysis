package com.djt.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * @author dajiangtai
 * @create 2020-02-04-9:18
 */
public class FlinkBatchProcessor {
    public static void main(String[] args) throws Exception {
        //第一步：获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //第二步：Create a TableEnvironment
        BatchTableEnvironment bTableEnv = BatchTableEnvironment.create(env);

        //读取数据
        DataSet<String> ds = env.readTextFile("D:\\study\\data\\2020\\user.txt");
        DataSet<Tuple2<String,String>> ds2 = ds.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2 map(String line) throws Exception {
                String[] split = line.split(",");
                return new Tuple2(split[0], split[1]);
            }
        });

        //第三步： Register a Table
        bTableEnv.registerDataSet("user1",ds2,"uid,name");

        //第四步：Query a Table
        //从Table API的查询中创建一个Table
        Table result = bTableEnv.scan("user1").select("name");
        //从SQL查询中创建一个Table
        //Table result = bTableEnv.sqlQuery("select name from user1");

        //table转化成DataStream
        DataSet<Row> rowDataSet = bTableEnv.toDataSet(result, Row.class);

        //打印输出
        rowDataSet.print();

        //第五步：Emit a Table
        TableSink sink = new CsvTableSink("D:\\study\\data\\2020\\user_copy", "|");
        String[] fieldNames = { "name"};
        TypeInformation[] fieldTypes = { Types.STRING};
        bTableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink);
        result.insertInto("CsvSinkTable");

        //执行
        env.execute("FlinkBatchProcessor");
    }
}
