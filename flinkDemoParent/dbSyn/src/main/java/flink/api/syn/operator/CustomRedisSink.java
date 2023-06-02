package flink.api.syn.operator;

import com.samur.common.pool.RedisClusterPool;
import flink.api.syn.pojo.RedisRow;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 自定义 redis输出器
 */
public class CustomRedisSink extends RichSinkFunction<RedisRow[]> {
    private JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedisPool = RedisClusterPool.getPool();
    }

    @Override
    public void invoke(RedisRow[] rows, Context context) throws Exception {
        Jedis resource = jedisPool.getResource();

        try {
            // 少量
            for (RedisRow row : rows) {
                switch (row.getOptType()) {
                    case DELETE:
                        resource.del(row.getKey());
                        break;
                    default:
                        resource.hset(row.getKey(), row.getData());
                }
            }

            // 批量
        }
        finally {
            if(resource != null) {
                resource.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
