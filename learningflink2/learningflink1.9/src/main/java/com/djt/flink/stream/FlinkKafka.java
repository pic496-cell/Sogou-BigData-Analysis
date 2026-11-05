package com.djt.flink.stream;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

/**
 * @author dajiangtai
 * @create 2020-01-15-14:06
 */
public class FlinkKafka {
    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop3-1:9092");
        properties.setProperty("group.id", "test-group");

        //kafka消费者
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("input-topic", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(myConsumer);

        //......

        //kafka生产者
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                "hadoop3-1:9092",            // broker list
                "output-topic",                  // target topic
                new SimpleStringSchema());   // serialization schema
        stream.addSink(myProducer);

        env.execute("StreamingFromCollection");


    }
}
