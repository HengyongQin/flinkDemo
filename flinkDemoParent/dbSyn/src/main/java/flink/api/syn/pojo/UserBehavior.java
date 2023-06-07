package flink.api.syn.pojo;

import lombok.Data;

@Data
public class UserBehavior {
    private long userId;

    private long itemId;

    private int categoryId;

    private BehaviorType behavior;

    private long timestamp;

    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = getBehaviorType(behavior);
        this.timestamp = timestamp;
    }

    private BehaviorType getBehaviorType(String behavior) {
        switch (behavior.toLowerCase()) {
            case "pv":
                return BehaviorType.PV;
            case "buy":
                return BehaviorType.BUY;
            case "cart":
                return BehaviorType.CART;
            case "fav":
                return BehaviorType.FAV;
        }

        return null;
    }

    public enum BehaviorType{
        PV, BUY, CART, FAV
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior=" + behavior +
                ", timestamp=" + timestamp +
                '}';
    }
}
