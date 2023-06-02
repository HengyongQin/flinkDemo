package flink.api.syn.pojo;

import com.samur.common.pojo.RowOptType;
import lombok.Getter;

import java.util.Map;

/**
 * redis 数据行
 */
public class RedisRow extends SinkRow {
    public RedisRow(String key, Map<String, String> data, RowOptType optType, long ts) {
        super(optType, ts);
        this.key = key;
        this.data = data;
        this.optType = optType;
        this.dataType = DataType.HASH;
    }

    public RedisRow(String key, String data, RowOptType optType, long ts) {
        super(optType, ts);
        this.key = key;
        this.data = data;
        this.optType = optType;
        this.dataType = DataType.STRING;
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
}
