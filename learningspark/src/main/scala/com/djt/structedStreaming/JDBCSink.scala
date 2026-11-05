package com.djt.structedStreaming

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.sql.{ForeachWriter, Row}

class JDBCSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {
  var conn: Connection = _
  var stmt: PreparedStatement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      conn = DriverManager.getConnection(url, user, pwd)
      stmt = conn.prepareStatement(
        "INSERT INTO newscount(name, count) VALUES(?, ?) " +
          "ON DUPLICATE KEY UPDATE count = VALUES(count)"
      )
      true
    } catch {
      case e: Exception =>
        println(s"======== [newscount] open失败: ${e.getMessage} ========")
        e.printStackTrace()
        throw e
    }
  }

  override def process(row: Row): Unit = {
    try {
      val name = row.getAs[String]("name")
      val count = row.getAs[Long]("count").toInt
      println(s">>>>> [newscount] 写入 name=$name, count=$count")
      stmt.setString(1, name)
      stmt.setInt(2, count)
      stmt.executeUpdate()
    } catch {
      case e: Exception =>
        println(s"======== [newscount] process失败: ${e.getMessage} ========")
        e.printStackTrace()
        throw e
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (stmt != null) stmt.close()
    if (conn != null) conn.close()
  }
}