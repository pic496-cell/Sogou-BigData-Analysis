package com.djt.flink.stream;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author dajiangtai
 * @create 2020-01-15-16:32
 */
public class JedisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.20.181", 6379);
        List<String> list1 = jedis.lrange("list1", 0, -1);
        for(String word:list1) {
            System.out.println(word);
        }
    }
}
