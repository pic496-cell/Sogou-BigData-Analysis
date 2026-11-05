package com.djt.sql

import org.apache.spark.sql._
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

/**
  * spark sql与mysql 集成大数据项目离线分析
  */
object sparksql_mysql {
  case class sogoulogs(logtime:String,uid:String,keywords:String,resultno:String,url:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("sogoulogs")
      .master("local[2]")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // 读取元数据
    val fileRDD = spark.sparkContext.textFile("F:\\labb\\sogouq.sample")

    // rdd 转DataSet
    val ds = fileRDD
      .map(line => line.split("\\t"))
      .filter(_.length == 5)
      .map(t => sogoulogs(t(0), t(1), t(2), t(3), t(4)))
      .toDS()

    ds.createTempView("sogoulogs")

    // 统计每个新闻浏览量
    val newsCount = spark.sql("select keywords as name ,count(keywords) as count from sogoulogs group by keywords")
    newsCount.show()
    newsCount.rdd.foreachPartition(myFun)

    // 统计每个时段新闻浏览量
    val periodCount = spark.sql("select logtime,count(logtime) as count from sogoulogs group by logtime")
    periodCount.show()
    periodCount.rdd.foreachPartition(myFun2)

  } // 🌟 main方法在这里闭合

  /**
    * 新闻浏览量数据插入mysql
    */
  def myFun(records: Iterator[Row]): Unit = {
    var conn: Connection = null
    var statement: Statement = null

    try {
      val url = Constants.url
      val userName: String = Constants.userName
      val passWord: String = Constants.passWord

      // conn 长连接
      conn = DriverManager.getConnection(url, userName, passWord)

      records.foreach(t => {
        // 清理关键词：去方括号、去反斜杠、替换单引号、截断长度
        val name = t.getAs[String]("name")
          .replaceAll("[\\[\\]]", "")
          .replace("\\", "")
          .replace("'", "''")
          .take(100)

        val count = t.getAs[Long]("count").asInstanceOf[Int]
        print(name + "@" + count + "***********************************")

        val sql = "select 1 from newscount where name = '" + name + "'"
        val updateSql = "update newscount set count = count+" + count + " where name ='" + name + "'"
        val insertSql = "insert into newscount(name,count) values('" + name + "'," + count + ")"

        var stmt: Statement = null
        var resultSet: java.sql.ResultSet = null

        try {
          stmt = conn.createStatement()
          resultSet = stmt.executeQuery(sql)

          if (resultSet.next()) {
            print("*****************更新******************")
            stmt.executeUpdate(updateSql)
          } else {
            print("*****************插入******************")
            stmt.execute(insertSql)
          }
        } finally {
          if (resultSet != null) resultSet.close()
          if (stmt != null) stmt.close()
        }
      }) // 🌟 records.foreach 在这里闭合
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (statement != null) statement.close()
      if (conn != null) conn.close()
    }
  } // 🌟 myFun 方法在这里闭合

  /**
    * 时段浏览量数据插入mysql数据
    */
  def myFun2(records: Iterator[Row]): Unit = {
    var conn: Connection = null
    var statement: Statement = null

    try {
      val url = Constants.url
      val userName: String = Constants.userName
      val passWord: String = Constants.passWord

      conn = DriverManager.getConnection(url, userName, passWord)

      records.foreach(t => {
        val logtime = t.getAs[String]("logtime")
        val count = t.getAs[Long]("count").asInstanceOf[Int]
        print(logtime + "@" + count + "***********************************")

        val sql = "select 1 from periodcount where logtime = '" + logtime + "'"
        val updateSql = "update periodcount set count = count+" + count + " where logtime ='" + logtime + "'"
        val insertSql = "insert into periodcount(logtime,count) values('" + logtime + "'," + count + ")"

        var stmt: Statement = null
        var resultSet: java.sql.ResultSet = null

        try {
          stmt = conn.createStatement()
          resultSet = stmt.executeQuery(sql)

          if (resultSet.next()) {
            print("*****************更新******************")
            stmt.executeUpdate(updateSql)
          } else {
            print("*****************插入******************")
            stmt.execute(insertSql)
          }
        } finally {
          if (resultSet != null) resultSet.close()
          if (stmt != null) stmt.close()
        }
      }) // 🌟 records.foreach 在这里闭合
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (statement != null) statement.close()
      if (conn != null) conn.close()
    }
  } // 🌟 myFun2 方法在这里闭合

} // 🌟 object sparksql_mysql 在这里闭合