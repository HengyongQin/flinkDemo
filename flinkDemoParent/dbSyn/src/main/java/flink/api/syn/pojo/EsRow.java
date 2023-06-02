package flink.api.syn.pojo;

import com.samur.common.pojo.RowOptType;
import lombok.Getter;

/**
 * 写到es 的数据行
 */
public class EsRow extends SinkRow {
    public EsRow(String index, String id, String data, RowOptType optType, long ts) {
        super(optType, ts);
        this.index = index;
        this.id = id;
        this.data = data;
    }

    /**
     * es存储index
     */
    @Getter
    private String index;

    /**
     * es存储 id
     */
    @Getter
    private String id;

    /**
     * es存储数据，json格式
     */
    @Getter
    private String data;
}
