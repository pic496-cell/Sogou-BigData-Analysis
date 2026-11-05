package com.djt.structedStreaming

object Constants {
  var url: String = "jdbc:mysql://hadoop001:3306/test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=utf8"
  var userName: String = "hive"
  var passWord: String = "hive"
  var driver: String = "com.mysql.cj.jdbc.Driver"

  var bootstrapServers: String = "hadoop001:9092,hadoop002:9092,hadoop003:9092"
  var topic: String = "test_ok"     // ← 已改
}