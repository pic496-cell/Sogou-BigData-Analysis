package com.djt.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * 插入数据
 * RPUSH list1 hadoop
 * RPUSH list1 spark
 * RPUSH list1 flink
 *
 *查看所有数据
 * LRANGE list2 0 -1
 *
 * 查看所有key
 * keys *
 */
//单并行度的source
public class RedisSource implements SourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSource.class);
    private Jedis jedis = null;
    private boolean isRunning =true ;
    private final long sleepTime = 5000;

    public void run(SourceContext<String> sourceContext) {
        this.jedis= new Jedis("192.168.20.181",6379);
        while(isRunning){
            try{
                //获取redis数据
                String line = jedis.lpop("list1");
                if(null != line){
                    sourceContext.collect(line);
                }
                Thread.sleep(sleepTime);
            }catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }

    public void cancel() {
        isRunning = false;
        if(jedis != null) {
            jedis.close();
        }
    }
}