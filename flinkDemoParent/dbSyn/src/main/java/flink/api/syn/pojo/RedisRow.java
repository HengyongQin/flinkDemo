package flink.api.syn.pojo;

import com.samur.common.pojo.RowOptType;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * redis 数据行
 */
public class RedisRow extends SinkRow {
    public RedisRow(String key, Map<String, String> data, RowOptType optType, long ts) {
        this(key, data, optType, ts, DataType.HASH);
    }

    public RedisRow(String key, String data, RowOptType optType, long ts) {
        this(key, data, optType, ts, DataType.STRING);
    }

    private RedisRow(String key, Object data, RowOptType optType, long ts, DataType dataType) {
        super(optType, ts);
        this.key = key;
        this.data = data;
        this.optType = optType;
        this.dataType = dataType;
    }

    /**
     * redis key
     */
    @Getter
    private String key;

    /**
     * redis value， hash结构
     */
    @Getter
    private Object data;

    @Getter
    private DataType dataType;

    public enum DataType {
        HASH, STRING
    }

    @Setter
    @Getter
    private long expireTime = -1;
}
