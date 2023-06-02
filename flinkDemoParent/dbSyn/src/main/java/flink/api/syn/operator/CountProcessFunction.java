package flink.api.syn.operator;

import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
import com.samur.common.utils.DateUtils;
import flink.api.syn.pojo.RedisRow;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class CountProcessFunction extends ProcessFunction<MysqlRow, RedisRow[]> {
    private static final String PRODUCT_COUNT_STATE_NAME = "product_count";
    private static final String PRODUCT_NO = "product_no";
    private static final String RESULT_KEY = "product_count_result";

    /**
     * 统计产品访问量
     */
    private MapState<String, Integer> productCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> state = new MapStateDescriptor<>(PRODUCT_COUNT_STATE_NAME, String.class, Integer.class);
        StateTtlConfig stateConfig = StateTtlConfig.newBuilder(Time.days(2))
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(3000)
                .build();
        state.enableTimeToLive(stateConfig);

        RuntimeContext context = getRuntimeContext();
        productCountState = context.getMapState(state);
    }

    @Override
    public void processElement(MysqlRow row, ProcessFunction<MysqlRow, RedisRow[]>.Context ctx, Collector<RedisRow[]> out) throws Exception {
        RowOptType optType = row.getOptType();
        Map<String, Object> after = row.getAfter();
        String productNo = (String) after.get(PRODUCT_NO);
        String productCountKey = createProductCountKey(productNo);
        Integer count = productCountState.get(productCountKey);
        count = optType.equals(RowOptType.DELETE) ? (count - 1) : (count + 1);
        productCountState.put(productCountKey, count);

        //---------to do -------//
        // 获取所有的 产品编码、产品名称
        //--------------------//

        out.collect(new RedisRow[]{new RedisRow(RESULT_KEY, count.toString(), RowOptType.INSERT, System.currentTimeMillis())});
    }

    private String createProductCountKey(String productNo) {
        return DateUtils.getCurrentDate() + productNo;
    }
}
