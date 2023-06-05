package flink.api.syn.operator;

import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
import com.samur.common.utils.DateUtils;
import flink.api.syn.dim.ProductDimTable;
import flink.api.syn.pojo.RedisRow;
import flink.api.util.StateUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class CountProcessFunction extends KeyedProcessFunction<String, MysqlRow, RedisRow[]> {
    private static final String ALL_COUNT_STATE_NAME = "all_count";
    private static final String PRODUCT_COUNT_STATE_NAME = "product_count";
    private static final String PRODUCT_NO = "product_id";

    /**
     * 统计所有产品订单量
     */
    private MapState<String, Integer> allCountState;

    /**
     * 统计每个产品订单量
     */
    private MapState<String, Map<String, Integer>> productCountState;

    private ProductDimTable products;

    @Override
    public void open(Configuration parameters) throws Exception {
        allCountState = StateUtil.createIntegerState(ALL_COUNT_STATE_NAME, getRuntimeContext(), 2);
        productCountState = StateUtil.createMapState(PRODUCT_COUNT_STATE_NAME, getRuntimeContext(), 2);
        products = new ProductDimTable(10 * 1000); // 10 秒钟过期;
    }

    @Override
    public void processElement(MysqlRow row, KeyedProcessFunction<String, MysqlRow, RedisRow[]>.Context ctx, Collector<RedisRow[]> out) throws Exception {
        out.collect(new RedisRow[]{
                countAll(row),
                countProductCount(row)});
    }

    /**
     * 统计各个产品订单数量
     * @return
     * @param row
     */
    private RedisRow countProductCount(MysqlRow row) throws Exception {
        String currentDate = DateUtils.getCurrentDate();
        String productNo = getProductKey(row);
        String productName = products.getProductName(productNo);
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
     * 统计所有产品订单量
     * @param row
     * @return
     * @throws Exception
     */
    private  RedisRow countAll(MysqlRow row) throws Exception {
        String currentDate = DateUtils.getCurrentDate();
        RowOptType optType = row.getOptType();
        Integer count = allCountState.get(currentDate);
        count = count == null ? 0 : count;
        count = optType.equals(RowOptType.DELETE) ? (count - 1)
                : optType.equals(RowOptType.UPDATE) ? 0 : (count + 1);
        allCountState.put(currentDate, count);

        return new RedisRow(ALL_COUNT_STATE_NAME, count.toString(), RowOptType.INSERT, System.currentTimeMillis());
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

    private static String createProductCountKey(String productNo) {
        return DateUtils.getCurrentDate() + productNo;
    }
}
