package flink.api.syn.operator;

import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
import com.samur.common.utils.DateUtils;
import flink.api.syn.pojo.RedisRow;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadCountProcessFunction extends BroadcastProcessFunction<MysqlRow, MysqlRow, RedisRow[]> {
    private static final String PRODUCT_COUNT_STATE_NAME = "product_count";
    private static final String PRODUCT_NO = "product_id";
    private static final String PRODUCT_NAME = "product_name";
    private static final Map<String, String> dimMap = new HashMap<>();

    /**
     * 统计每个产品订单量
     */
    private Map<String, Map<String, Integer>> productCountState = new HashMap<>();// k-产品名称 productName, v-count

    @Override
    public void open(Configuration parameters) throws Exception {
        dimMap.put("p_1", "小神童");
        dimMap.put("p_2", "达尔文");
        dimMap.put("p_3", "守卫者");
    }

    @Override
    public void processElement(MysqlRow row, BroadcastProcessFunction<MysqlRow, MysqlRow, RedisRow[]>.ReadOnlyContext ctx, Collector<RedisRow[]> out) throws Exception {
        RedisRow redisRow = countProductCount(row);
        out.collect(new RedisRow[]{redisRow});
    }

    @Override
    public void processBroadcastElement(MysqlRow row, BroadcastProcessFunction<MysqlRow, MysqlRow, RedisRow[]>.Context ctx, Collector<RedisRow[]> out) throws Exception {
        System.out.println("流广播被触发！");
        Map<String, Object> nData = row.getData();
        String productNo = (String) nData.get(PRODUCT_NO);
        String productName = (String) nData.get(PRODUCT_NAME);

        if(!dimMap.containsKey(productNo)) {
            dimMap.put(productNo, productName);
            return;
        }

        String oldProductName = dimMap.get(productNo);

        if(!oldProductName.equals(productName)) {
            dimMap.put(productNo, productName);
            Map<String, Integer> state = productCountState.get(DateUtils.getCurrentDate());

            if(state != null && state.containsKey(oldProductName)) {
                Integer count = state.get(oldProductName);
                state.remove(oldProductName);
                state.put(productName, count);

                Map<String, Double> result = new HashMap<>();
                state.forEach((k, v) -> result.put(k, Double.valueOf(v)));
                out.collect(new RedisRow[]{
                        new RedisRow(result, PRODUCT_COUNT_STATE_NAME, RowOptType.DELETE, System.currentTimeMillis()),
                        new RedisRow(result, PRODUCT_COUNT_STATE_NAME, RowOptType.INSERT, System.currentTimeMillis())});
            }
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
        String productName = dimMap.get(productNo);
        productName = productName == null ? "null" : productName;
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
