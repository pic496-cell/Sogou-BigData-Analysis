package com.djt.flink.state;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class TestOperatorState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置checkpoint
        env.enableCheckpointing(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop3cluster/ops/flink-stream-test/auditlog-alert/checkpoint"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        DataStream<Long> inputStream=env.fromElements(1L,2L,3L,4L,5L,1L,3L,4L,5L,6L,7L,1L,4L,5L,3L,9L,9L,2L,1L);

        inputStream.flatMap(new CountWithOperatorState())
                .setParallelism(1)
                .print();

        env.execute();

    }
}
