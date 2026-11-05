package com.djt.flink.news;
import java.util.Properties;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
public class KafkaFlinkMySQL {
    public static void main(String[] args) throws Exception {
    	//获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "sogoulogs");

        //kafka消费者
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("sogoulogs", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(myConsumer);
        
        //对数据进行过滤
        DataStream<String> filter = stream.filter((value) -> value.split(",").length==6);
        
        DataStream<Tuple2<String, Integer>> newsCounts = filter.flatMap(new LineSplitter()).keyBy(0).sum(1);
        //自定义MySQL sink
        newsCounts.addSink(new MySQLSink());
        
        DataStream<Tuple2<String, Integer>> periodCounts = filter.flatMap(new LineSplitter2()).keyBy(0).sum(1);
        //自定义MySQL sink
        periodCounts.addSink(new MySQLSink2());

        // 执行flink 程序
        env.execute("FlinkMySQL");
    }
    
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split(",");
			out.collect(new Tuple2<String, Integer>(tokens[2], 1));
		}
	}
    
    public static final class LineSplitter2 implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split(",");
			out.collect(new Tuple2<String, Integer>(tokens[0], 1));
		}
	}

}
