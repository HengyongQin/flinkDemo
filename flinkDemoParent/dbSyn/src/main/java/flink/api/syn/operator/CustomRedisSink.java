package flink.api.syn.operator;

import com.samur.common.pojo.RowOptType;
import com.samur.common.pool.RedisClusterPool;
import com.samur.common.utils.DateUtils;
import flink.api.syn.pojo.RedisRow;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * 自定义 redis输出器
 */
public class CustomRedisSink extends RichSinkFunction<RedisRow[]> {
    private JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("打开CustomRedisSink 算子");
        super.open(parameters);
        jedisPool = RedisClusterPool.getPool();
    }

    @Override
    public void invoke(RedisRow[] rows, Context context) throws Exception {
        Jedis jedis = jedisPool.getResource();
        System.out.println("从线程池获取连接！！" + jedis);
        System.out.println("写入数据：");
        for (RedisRow row : rows) {
            System.out.println(row.toString());
        }

        try {
            for (RedisRow row : rows) {
                RedisRow.DataType dataType = row.getDataType();
                Object data = row.getData();

                if(row.getOptType() == RowOptType.DELETE) {
                    jedis.del(row.getKey());
                }
                else if (dataType.equals(RedisRow.DataType.HASH)){
                    writeData(jedis, row.getKey(), (Map<String, String>) data);
                }
                else if(dataType.equals(RedisRow.DataType.Z_SET)) {
                    jedis.zadd(row.getKey(), (Map<String, Double>)row.getData());
                }
                else  {
                    writeData(jedis, row.getKey(), (String) data);
                }

                setExpire(row, jedis);
            }
        }
        finally {
            if(jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 写入数据
     * @param resource
     * @param key
     * @param value
     */
    private void writeData(Jedis resource, String key, String value) {
        resource.set(key, value);
    }

    /**
     * 写入数据
     * @param resource
     * @param key
     * @param value
     */
    private void writeData(Jedis resource, String key, Map<String, String> value) {
        resource.hset(key, value);
    }

    /**
     * 设置过期时间
     */
    private void setExpire(RedisRow row, Jedis jedis) {
        long expireTime = row.getExpireTime();

        if(expireTime > 0 && row.getOptType() != RowOptType.DELETE) {
            jedis.expireAt(row.getKey(), expireTime);
        }
    }
}
