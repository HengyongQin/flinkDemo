package flink.api.syn.pojo;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class ItemCount implements Serializable, Comparable<ItemCount> {
    private static final long serialVersionUID = 1L;

    private long itemId;

    private Integer count;

    private long ts;

    public ItemCount(long itemId, int count, long ts) {
        this.itemId = itemId;
        this.count = count;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ItemCount{" +
                "itemId=" + itemId +
                ", count=" + count +
                ", ts=" + new Timestamp(ts) +
                '}';
    }

    @Override
    public int compareTo(ItemCount n) {
        return n.count.equals(this.count) ? Long.compare(n.itemId, itemId) : Integer.compare(n.count, count);
    }
}
