package flink.api.syn.operator;

import flink.api.syn.pojo.RedisRow;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 自定义 redis输出器
 */
public class CustomRedisSink extends RichSinkFunction<RedisRow> {
    private JedisPool jedisPool;
    private String redisHost;
    private int redisPort;

    public CustomRedisSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setMinIdle(1);
        config.setTestOnBorrow(true);
        jedisPool = new JedisPool(config, redisHost, redisPort);
    }

    @Override
    public void invoke(RedisRow row, Context context) throws Exception {
        Jedis resource = jedisPool.getResource();

        try {
            switch (row.getOptType()) {
                case DELETE:
                    resource.del(row.getKey());
                    break;
                default:
                    resource.hset(row.getKey(), row.getData());
            }
        }
        finally {
            if(resource != null) {
                resource.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        jedisPool.close();
        super.close();
    }
}
