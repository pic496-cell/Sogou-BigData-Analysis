package com.djt.flink.time;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author dajiangtai
 * @create 2020-02-21-18:28
 */
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置time类型，使用eventtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //设置并行度为1 便于分析
        env.setParallelism(1);

        //输入数据
        DataStream<String> ds = env.socketTextStream("hadoop3-1", 9999, "\n");

        //数据转换为tuple
        DataStream<Tuple2<String, Long>> ds2 = ds.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], Long.parseLong(split[1]));
            }
        });

        //周期性生成watermark
        DataStream<Tuple2<String, Long>> waterMark = ds2.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            //当前最大时间矬
            Long currentMaxTimestamp = 0L;
            //最大允许的乱序时间是10s
            final Long maxOutOfOrderness = 10000L;

            //转换时间格式，方便查看
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            /**
             *重写方法生成watermark
             * 默认100ms被调用一次
             * @return
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            /**
             * 重写方法提取timestamp
             * @param element
             * @param previousElementTimestamp
             * @return
             */
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                //每输入一条数据，打印【key，eventtime，currentMaxTimestamp，watermark】
                System.out.println("key:"+element.f0+",eventtime:["+sdf.format(element.f1)+"],currentMaxTimestamp:["+
                        sdf.format(currentMaxTimestamp)+"],watermark:["+sdf.format(getCurrentWatermark().getTimestamp())+"]");
                return timestamp;
            }
        });

        //保存被丢弃的数据
        OutputTag<Tuple2<String, Long>> lateOutputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};

        SingleOutputStreamOperator<String> ds3 = waterMark.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //.allowedLateness(Time.seconds(3))
                .sideOutputLateData(lateOutputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     * 对window内的数据进行排序，保证数据的顺序
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        //获取key值
                        String key = tuple.toString();
                        List<Long> list = new ArrayList<Long>();
                        //将窗口所有元素放入list集合
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long>  t= it.next();
                            list.add(t.f1);
                        }
                        //对窗口所有元素排序
                        Collections.sort(list);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                        String windowResult = key + ",窗口元素个数:[" + list.size() + "],窗口元素最小值:[" + sdf.format(list.get(0)) + "],窗口元素最大值:[" + sdf.format(list.get(list.size() - 1))
                                + "],窗口起始值:[" + sdf.format(window.getStart()) + "],窗口结束值:[" + sdf.format(window.getEnd())+"]";
                        out.collect(windowResult);
                    }
                });
        //获取迟到的数据打印控制台，生产环境中可以输出到kafka等其他系统
        DataStream<Tuple2<String, Long>> lateStream = ds3.getSideOutput(lateOutputTag);
        lateStream.print();

        ds3.print();
        env.execute("WatermarkDemo");
    }
}
