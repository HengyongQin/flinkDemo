package com.samur.common.pool;

import com.samur.common.properties.PropertiesConstant;
import com.samur.common.properties.PropertiesHelper;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * redis 线程池
 */
public class RedisClusterPool {
    private static JedisPool pool;
    private static final Lock lock = new ReentrantLock();

    public static JedisPool getPool() {
        if(pool == null) {
            lock.lock();

            try {
                if(pool == null) {
                    System.out.println("开始创建线程池！！");
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(10);
                    config.setMaxIdle(5);
                    config.setMinIdle(1);
                    config.setTestOnBorrow(true);
                    pool = new JedisPool(config
                            , PropertiesHelper.getValue(PropertiesConstant.REDIS_HOST)
                            , PropertiesHelper.getInt(PropertiesConstant.REDIS_PORT));
                    System.out.println("创建线程池成功~~~~");
                }
            }
            finally {
                lock.unlock();
            }

        }

        return pool;
    }
}
