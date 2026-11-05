package com.djt.flink.stream;

import com.djt.flink.model.Offset;
import com.djt.flink.sink.HbaseSyncSink;
import com.djt.flink.source.HBaseSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author dajiangtai
 * @create 2020-01-16-16:19
 */
public class FlinkHBase {
    public static void main(String[] args) throws Exception {
        //获取一个执行环境，本地或者集群环境会自动识别
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //自定义
        DataStream<Offset> dataStream = env.addSource(new HBaseSource());

        //....实现逻辑

        //自定义MySQL sink
        dataStream.addSink(new HbaseSyncSink());

        // 执行flink 程序
        env.execute("FlinkHBase");
    }
}
