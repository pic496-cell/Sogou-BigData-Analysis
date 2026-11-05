package com.djt.flink.news;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
public class MySQLSink2 extends RichSinkFunction<Tuple2<String, Integer>> {
	private Connection connection;
    private PreparedStatement preparedStatement;
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(GlobalConfig.DRIVER_CLASS);
        // 获取数据库连接
        connection = DriverManager.getConnection(GlobalConfig.DB_URL, GlobalConfig.USER_MAME, GlobalConfig.PASSWORD);      
    }
 
    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }
 
    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        try {
        	String  logtime = value.f0;
            int count = value.f1;
            
        	String querySQL="select 1 from periodcount "+" where logtime = '"+logtime+"'";
        	
        	String updateSQL="update periodcount set count = "+count+" where logtime ='"+logtime+"'";
        	
        	String insertSQL = "insert into periodcount(logtime,count) values('"+logtime+"',"+count+")";    	
        	//查询数据
        	preparedStatement = connection.prepareStatement(querySQL);
        	ResultSet resultSet = preparedStatement.executeQuery();
        	if(resultSet.next()){
        		//更新数据
        		preparedStatement.executeUpdate(updateSQL);
        	}else{
        		//插入数据
        		preparedStatement.execute(insertSQL);
        	}
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
