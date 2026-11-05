package com.djt.flink.stream;
import com.djt.flink.sink.MySQLSink;
import com.djt.flink.source.MySQLSource;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author dajiangtai
 * @create 2020-01-16-10:19
 */
public class FlinkMySQL {
    public static void main(String[] args) throws Exception {
        //获取一个执行环境，本地或者集群环境会自动识别
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //自定义MySQL source
        DataStream<Tuple7<Integer,Integer,String,String,String,String, Integer>> dataStream = env.addSource(new MySQLSource());


        //....业务实现

        //自定义MySQL sink
        dataStream.addSink(new MySQLSink());

        // 执行flink 程序
        env.execute("FlinkMySQL");
    }

}
