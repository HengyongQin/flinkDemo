package flink.api.syn.pojo;

import com.samur.common.pojo.RowOptType;
import lombok.Getter;

public abstract class SinkRow {
    /**
     * 操作类型
     */
    @Getter
    public RowOptType optType;

    /**
     * 数据时间产生戳
     */
    private long ts;

    public SinkRow(RowOptType optType, long ts) {
        this.optType = optType;
        this.ts = ts;
    }

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
