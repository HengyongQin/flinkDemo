package flink.api.syn.dim;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.samur.common.pool.RedisClusterPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public abstract class AbstractRedisDimTable {
    /**
     * 是否自动刷新
     */
    protected boolean autoRefresh;

    /**
     * 数据过期时间，单位：毫秒。如果设置为0，则不走缓存
     */
    protected long expireTime;

    /**
     * 最大容量
     */
    protected long maxSize = 10240;

    private JedisPool pool = RedisClusterPool.getPool();
    private LoadingCache<String, Map<String, String>> cache;

    protected AbstractRedisDimTable(long expireTime, boolean autoRefresh) {
        this.expireTime = expireTime;
        this.autoRefresh = autoRefresh;
        init();
    }

    protected AbstractRedisDimTable(long expireTime) {
        this(expireTime, false);
    }

    /**
     * 初始化，创建缓存
     */
    private void init() {
        if(expireTime > 0) {
            CacheBuilder builder = CacheBuilder.newBuilder().maximumSize(maxSize);
            builder = autoRefresh ? builder.refreshAfterWrite(Duration.ofMillis(expireTime)) : builder.expireAfterWrite(Duration.ofMillis(expireTime));
            cache = builder.build(new CacheLoader<String, Map<String, String>>() {
                        @Override
                        public Map<String, String> load(String key) throws Exception {
                            System.out.println("访问redis，获取key值：" + key);
                            return getValueFromRedis(key);
                        }
                    });
        }
    }

    /**
     * 获取值
     * @param key
     * @return
     * @throws ExecutionException
     */
    protected Map<String, String> getValue(String key) throws ExecutionException {
        return cache == null ? getValueFromRedis(key) : cache.get(key);
    }

    /**
     * 获取redis的值
     * @param key
     * @return
     */
    private Map<String, String> getValueFromRedis(String key) {
        Jedis resource = pool.getResource();
        Map<String, String> redisValue = new HashMap<>();

        try {
            redisValue.putAll(resource.hgetAll(key));
        }
        finally {
            if(resource != null) resource.close();
        }

        return redisValue;
    }

    /**
     * 获取redis的值
     * @param key
     * @param columns
     * @return
     */
    private Map<String, String> getValueFromRedis(String key, String[] columns) {
        Map<String, String> redisValue = getValueFromRedis(key);
        Map<String, String> data = new HashMap<>();

        for (String column : columns) {
            data.put(column, redisValue.get(column));
        }

        return data;
    }
}
