package com.djt.structedStreaming

import java.sql.{Connection, DriverManager}
import java.util

class MySqlPool(url: String, user: String, pwd: String) extends Serializable {
  private val max = 5
  private var conNum = 0
  private val pool = new util.LinkedList[Connection]()

  def getJdbcConn(): Connection = {
    AnyRef.synchronized({
      // 加载驱动放在这里，只会在第一次创建时触发
      Class.forName(Constants.driver)

      if (pool.isEmpty) {
        val conn = DriverManager.getConnection(url, user, pwd)
        pool.push(conn)
        conNum += 1
      }
      pool.poll()
    })
  }

  def releaseConn(conn: Connection): Unit = {
    pool.push(conn)
  }
}