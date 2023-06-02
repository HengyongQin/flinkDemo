package flink.api.syn.operator;

import com.samur.common.pojo.RowOptType;
import com.samur.common.pool.RedisClusterPool;
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
        Jedis resource = jedisPool.getResource();
        System.out.println("从线程池获取连接！！" + resource);

        try {
            for (RedisRow row : rows) {
                RedisRow.DataType dataType = row.getDataType();
                Object data = row.getData();

                if(row.getOptType() == RowOptType.DELETE) {
                    resource.del(row.getKey());
                }
                else if (dataType.equals(RedisRow.DataType.HASH)){
                    writeData(resource, row.getKey(), (Map<String, String>) data);
                }
                else  {
                    writeData(resource, row.getKey(), (String) data);
                }
            }
        }
        finally {
            if(resource != null) {
                System.out.println("关闭连接：" + resource);
                resource.close();
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
}
