package com.djt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * flume+kafka+Spark Streaming+mysql集成开发
 */
object kafka_sparkStreaming_mysql {
   
  /**
   * 新闻浏览量数据插入mysql
   */
  def myFun(records:Iterator[(String,Int)]): Unit = {
    var conn:Connection = null
    var statement:Statement = null
    
    try{
      val url = Constants.url
      val userName:String = Constants.userName
      val passWord:String = Constants.passWord
      
      //conn长连接
      conn = DriverManager.getConnection(url, userName, passWord)
      
      records.foreach(t => {
        
        val name = t._1.replaceAll("[\\[\\]]", "")
        val count = t._2
        print(name+"@"+count+"***********************************")
        
        val sql = "select 1 from newscount "+" where name = '"+name+"'"
        
        val updateSql = "update newscount set count = count+"+count+" where name ='"+name+"'"
        
        val insertSql = "insert into newscount(name,count) values('"+name+"',"+count+")"
        //实例化statement对象
        statement = conn.createStatement()
        
        //执行查询
        var resultSet = statement.executeQuery(sql)
        
        if(resultSet.next()){
          print("*****************更新******************")
          statement.executeUpdate(updateSql)
        }else{
           print("*****************插入******************")
          statement.execute(insertSql)
        }
        
      })
    }catch{    
      case e:Exception => e.printStackTrace()
    }finally{
      if(statement !=null){
        statement.close()
      }
      
      if(conn !=null){
        conn.close()
      }
      
    }
    
  }
  
  /**
   * 时段浏览量数据插入mysql数据
   */
  def myFun2(records:Iterator[(String,Int)]): Unit = {
    var conn:Connection = null
    var statement:Statement = null
    
    try{
      val url = Constants.url
      val userName:String = Constants.userName
      val passWord:String = Constants.passWord
      
      //conn
      conn = DriverManager.getConnection(url, userName, passWord)
      
      records.foreach(t => {
        
        val logtime = t._1
        val count = t._2
        print(logtime+"@"+count+"***********************************")
        
        val sql = "select 1 from periodcount "+" where logtime = '"+logtime+"'"
        
        val updateSql = "update periodcount set count = count+"+count+" where logtime ='"+logtime+"'"
        
        val insertSql = "insert into periodcount(logtime,count) values('"+logtime+"',"+count+")"
        //实例化statement对象
        statement = conn.createStatement()
        
        //执行查询
        var resultSet = statement.executeQuery(sql)
        
        if(resultSet.next()){
          print("*****************更新******************")
          statement.executeUpdate(updateSql)
        }else{
           print("*****************插入******************")
          statement.execute(insertSql)
        }
        
      })
    }catch{    
      case e:Exception => e.printStackTrace()
    }finally{
      if(statement !=null){
        statement.close()
      }
      
      if(conn !=null){
        conn.close()
      }
      
    }
    
  }
  def main(args: Array[String]): Unit = {
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("sogoulogs").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sogoulogs",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    
    val topics = Array("sogoulogs")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    
    val lines = stream.map(record =>  record.value)
    
    //无效数据过滤
    val filter = lines.map(_.split(",")).filter(_.length==6)
    
    //统计所有新闻话题浏览量
    val newsCounts = filter.map(x => (x(2), 1)).reduceByKey(_ + _)
    newsCounts.foreachRDD(rdd => {
      print("--------------------------------------------")
      //分区并行执行
      rdd.foreachPartition(myFun)
    })
    newsCounts.print()
    
    //统计所有时段新闻浏览量
    val periodCounts = filter.map(x => (x(0), 1)).reduceByKey(_ + _)
    periodCounts.print()
    
    periodCounts.foreachRDD(rdd =>{
      rdd.foreachPartition(myFun2)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}