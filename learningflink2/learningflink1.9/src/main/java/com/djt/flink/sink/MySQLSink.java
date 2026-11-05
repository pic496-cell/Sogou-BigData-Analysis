package com.djt.flink.sink;
import com.djt.flink.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSink extends RichSinkFunction<Tuple7<Integer,Integer,String,String,String,String, Integer>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(GlobalConfig.DRIVER_CLASS);
        // 获取数据库连接
        connection = DriverManager.getConnection(GlobalConfig.DB_URL, GlobalConfig.USER_MAME, GlobalConfig.PASSWORD);
        preparedStatement = connection.prepareStatement(GlobalConfig.INSERTSQL);
        super.open(parameters);
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
    public void invoke(Tuple7<Integer,Integer,String,String,String,String, Integer> value, Context context) throws Exception {
        try {
            int flowId = value.f0;
            int mode = value.f1;
            String databaseName = value.f2;
            String tableName = value.f3;
            String hbaseTable = value.f4;
            String family = value.f5;
            int commitBatch = value.f6;
            preparedStatement.setInt(1,flowId);
            preparedStatement.setInt(2,mode);
            preparedStatement.setString(3,databaseName);
            preparedStatement.setString(4,tableName);
            preparedStatement.setString(5,hbaseTable);
            preparedStatement.setString(6,family);
            preparedStatement.setInt(7,commitBatch);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
