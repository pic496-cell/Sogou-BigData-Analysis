package com.djt.flink.BACD;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
public class BroadcastDemo {

    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1.准备需要广播的数据
        ArrayList<Tuple2< Integer,String>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>(101,"beijing"));
        broadData.add(new Tuple2<>(102,"shanghai"));
        broadData.add(new Tuple2<>(103,"hubei"));
        DataSet<Tuple2<Integer,String>> tupleData = env.fromCollection(broadData);


        //2.处理需要广播的数据,把数据集转换成map类型
        DataSet<HashMap<Integer,String>> toBroadcast = tupleData.map(new MapFunction<Tuple2<Integer,String>, HashMap<Integer,String>>() {
            @Override
            public HashMap<Integer,String> map(Tuple2<Integer,String> value) throws Exception {
                HashMap<Integer,String> res = new HashMap<>();
                res.put(value.f0, value.f1);
                return res;
            }
        });

        //3.源数据
        ArrayList<Tuple2<Integer, String>> dataSource = new ArrayList<>();
        dataSource.add(new Tuple2(101,"1.1万亿"));
        dataSource.add(new Tuple2(102,"1万亿"));
        dataSource.add(new Tuple2(103,"8千亿"));
        DataSource<Tuple2<Integer,String>> data = env.fromCollection(dataSource);

        //注意：在这里需要使用到RichMapFunction获取广播变量
        DataSet<String> result = data.map(new RichMapFunction<Tuple2<Integer, String>, String>() {
            //存储广播数据
            List<HashMap<Integer, String>> broadCastMap = new ArrayList<HashMap<Integer, String>>();
            HashMap<Integer, String> allMap = new HashMap<Integer, String>();

            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             *
             * 所以，就可以在open方法中获取广播变量数据
             *
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //5:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            /**
             * 数据源于广播数据匹配
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map(Tuple2<Integer, String> value) throws Exception {
                String province = allMap.get(value.f0);
                return province+","+value.f1;
            }
        }).withBroadcastSet(toBroadcast, "broadCastName");//4：执行广播数据的操作

        result.print();
    }
}
