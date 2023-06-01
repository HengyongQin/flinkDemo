package flink.api.syn.pojo;

import com.samur.common.pojo.RowOptType;
import lombok.Getter;

/**
 * 写到es 的数据行
 */
public class EsRow {
    private EsRow() {
    }

    public EsRow(String index, String id, String data, RowOptType optType, long ts) {
        this.index = index;
        this.id = id;
        this.data = data;
        this.optType = optType;
        this.ts = ts;
    }

    @Getter
    private String index;

    @Getter
    private String id;

    @Getter
    private String data;

    @Getter
    private RowOptType optType;

    private long ts;

    /**
     * 获取操作顺序，如果同一时刻 数据做了 增删改操作，操作顺序应为：删-增-改
     * @return
     */
    public long getPos() {
        short optIndex = 0;

        switch (optType) {
            case DELETE:
                optIndex = 1;
                break;
            case INSERT:
                optIndex = 2;
                break;
            case UPDATE:
                optIndex = 3;
        }

        return ts * 10 + optIndex;
    }
}
