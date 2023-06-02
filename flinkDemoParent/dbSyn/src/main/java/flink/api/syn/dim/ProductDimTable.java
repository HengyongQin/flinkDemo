package flink.api.syn.dim;

import com.samur.common.utils.KeyUtils;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 产品维度表
 */
public class ProductDimTable extends AbstractRedisDimTable {
    private final static String database = "flink";
    private final static String tableName = "product";
    private final static String PRODUCT_NAME_COLUMN = "product_name";

    public ProductDimTable(long expireTime) {
        super(expireTime);
    }

    public ProductDimTable(long expireTime, boolean autoRefresh) {
        super(expireTime, autoRefresh);
    }

    /**
     * 获取产品名称
     * @param productNo
     * @return
     */
    public String getProductName(String productNo) throws ExecutionException {
        Map<String, String> redisValue = getValue(KeyUtils.createRedisKey(database, tableName, productNo));
        return redisValue == null ? null : redisValue.get(PRODUCT_NAME_COLUMN);
    }
}
