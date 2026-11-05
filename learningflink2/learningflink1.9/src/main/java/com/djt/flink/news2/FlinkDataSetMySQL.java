package com.djt.flink.news2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
public class FlinkDataSetMySQL {
	public static void main(String[] args) throws Exception {
		//获取Flink的运行环境
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//读取数据
		DataSet<String> ds = env.readTextFile(args[0]);
		
		//对数据进行过滤
		DataSet<String> filter = ds.filter((value) -> value.split(",").length==6);

		//统计所有新闻话题浏览量
		DataSet<Tuple2<String, Integer>> newsCounts =
				filter.flatMap(new LineSplitter())
				.groupBy(0)
				.sum(1);
		//数据转换
		DataSet<Row> outputData =newsCounts.map(new MapFunction<Tuple2<String,Integer>, Row>() {
			public Row map(Tuple2<String,Integer> t) throws Exception {
				Row row = new Row(2);
				row.setField(0, t.f0.replaceAll("[\\[\\]]", ""));
				row.setField(1, t.f1.intValue());
				return row;
			};
		});

		//数据输出到newscount表
		outputData.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(GlobalConfig.DRIVER_CLASS)
                .setDBUrl(GlobalConfig.DB_URL)
                .setUsername(GlobalConfig.USER_MAME)
                .setPassword(GlobalConfig.PASSWORD)
                .setQuery(GlobalConfig.SQL)
                .finish());
        
		//统计所有时段新闻浏览量
        DataSet<Tuple2<String, Integer>> periodCounts = filter.flatMap(new LineSplitter2()).groupBy(0).sum(1);
        //数据转换
        DataSet<Row> outputData2 =periodCounts.map(new MapFunction<Tuple2<String,Integer>, Row>() {
			public Row map(Tuple2<String,Integer> t) throws Exception {
				Row row = new Row(2);
				row.setField(0, t.f0);
				row.setField(1, t.f1.intValue());
				return row;
			};
		});
        
        //数据输出到periodcount表
        outputData2.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(GlobalConfig.DRIVER_CLASS)
                .setDBUrl(GlobalConfig.DB_URL)
                .setUsername(GlobalConfig.USER_MAME)
                .setPassword(GlobalConfig.PASSWORD)
                .setQuery(GlobalConfig.SQL2)
                .finish());

        // 执行flink 程序
		env.execute("FlinkDataSetMySQL");


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
