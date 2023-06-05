package flink.api.syn.operator;

import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
import com.samur.common.utils.DateUtils;
import flink.api.syn.dim.ProductDimTable;
import flink.api.syn.pojo.RedisRow;
import flink.api.util.StateUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AsyncCountProcessFunction extends RichAsyncFunction<MysqlRow, RedisRow[]> {
    private static final String PRODUCT_COUNT_STATE_NAME = "product_count";
    private static final String PRODUCT_NO = "product_id";
    private ProductDimTable productDimTable;

    /**
     * 统计每个产品订单量
     */
    private Map<String, Map<String, Integer>> productCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        productDimTable = new ProductDimTable(10 * 1000);
        productCountState = new HashMap<>();// 不支持state，用hashmap代替
    }

    @Override
    public void asyncInvoke(MysqlRow row, ResultFuture<RedisRow[]> resultFuture) {
        try {
            RedisRow redisRow = countProductCount(row);
            List<RedisRow[]> list = new ArrayList<>();
            list.add(new RedisRow[]{redisRow});
            resultFuture.complete(list);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 统计各个产品订单数量
     * @return
     * @param row
     */
    private RedisRow countProductCount(MysqlRow row) throws Exception {
        String currentDate = DateUtils.getCurrentDate();
        String productNo = getProductKey(row);
        String productName = productDimTable.getProductName(productNo);
        RowOptType optType = row.getOptType();

        Map<String, Integer> state = productCountState.get(currentDate);
        state = state == null ? new HashMap<>() : state;

        Integer count = state.get(productName);
        count = count == null ? 0 : count;
        count = optType.equals(RowOptType.DELETE) ? (count - 1)
                : optType.equals(RowOptType.UPDATE) ? 0 : (count + 1);
        state.put(productName, count);
        productCountState.put(currentDate, state);

        Map<String, Double> result = new HashMap<>();
        state.forEach((k, v) -> result.put(k, Double.valueOf(v)));
        return new RedisRow(result, PRODUCT_COUNT_STATE_NAME, RowOptType.INSERT, System.currentTimeMillis());
    }

    /**
     * 获取产品编码
     * @param row
     * @return
     */
    private static String getProductKey(MysqlRow row) {
        Map<String, Object> data = row.getData();
        return (String) data.get(PRODUCT_NO);
    }
}
