package com.djt.flink.BACD;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @author dajiangtai
 * @create 2020-02-18-15:40
 */
public class CounterDemo {
    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("火神山", "雷神山", "钟南山", "方舱医院");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            //1:创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2:注册累加器
                getRuntimeContext().addAccumulator("num-lines",this.numLines);

            }

            int sum = 0;
            @Override
            public String map(String value) throws Exception {
                //如果并行度为1，使用普通的累加求和即可；如果并行度大于1，需要使用累加器累加
                sum++;
                System.out.println("sum："+sum);
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(4);

        //批处理中print()方法中已经包含execute，故后面不能重复调用。
        //result.print();

        result.writeAsText("D:\\study\\data\\2020\\count2");
        //想获取计数器，又必须显示调用execute获取返回结果
        JobExecutionResult jobResult = env.execute("CounterDemo");

        //3：获取累加器
        int numLines = jobResult.getAccumulatorResult("num-lines");
        System.out.println("numLines:"+numLines);

    }
}
