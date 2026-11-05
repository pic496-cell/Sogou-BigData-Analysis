package com.djt.flink.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author dajiangtai
 * @create 2020-02-04-13:03
 */
public class StreamProcessor {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建一个TableEnvironment
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(sEnv);
        //读取数据源
        DataStream<String> ds = sEnv.socketTextStream("hadoop3-1", 9999, "\n");

        //解析数据
        DataStream<WC> ds2 = ds.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String line, Collector<WC> out) throws Exception {
                String[] tokens = line.toLowerCase().split("\\W+");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new WC(token,1));
                    }
                }
            }
        });

        //DataStream转table
        Table table = sTableEnv.fromDataStream(ds2);

        sTableEnv.registerTable("wc",table);


        //Append Mode
        //如果数据只是不断添加，可以使用追加模式
        //DataStream<WC> wcDataStream = sTableEnv.toAppendStream(table, WC.class);



        //报错，不适合更新或者删除操作
        //Table table1 = sTableEnv.sqlQuery("select word ,count(frequency) from wc group by word");
        //DataStream<Row> wcDataStream = sTableEnv.toAppendStream(table1, Row.class);

        //Retract Mode
        Table table2 = sTableEnv.sqlQuery("select cnt,count(word) as freq from (select word,count(frequency) as cnt from wc group by word) group by cnt");
        DataStream<Tuple2<Boolean, Row>> wcDataStream = sTableEnv.toRetractStream(table2, Row.class);


        wcDataStream.print();

        sEnv.execute("TableToStream");
    }

    public static class WC{
        public String word;
        public Integer  frequency;
        //需要无参数构造方法
        public WC(){}

        public WC(String word,Integer frequency){
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }
}