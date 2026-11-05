package com.djt.flink.stream;


import com.djt.flink.batch.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 * 统计单词词频
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        //解析命令行传过来的参数
        final ParameterTool params = ParameterTool.fromArgs(args);

        // 获取一个执行环境，本地或者集群环境会自动识别
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // 读取输入数据
        DataStream<String> text;
        if (params.has("input")) {
            // 读取text文件
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // 读取默认测试数据集
            text = env.fromElements(WordCountData.WORDS);
        }

        DataStream<Tuple2<String, Integer>> counts =
                // 切分每行单词
                text.flatMap(new Tokenizer())
                        //对每个单词分组统计词频数
                        .keyBy(0).sum(1);

        // 输出统计结果
        if (params.has("output")) {
            //写入文件地址
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            //数据打印控制台
            counts.print();
        }

        // 执行flink 程序
        env.execute("Streaming WordCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}
