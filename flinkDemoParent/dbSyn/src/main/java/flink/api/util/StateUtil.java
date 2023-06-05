package flink.api.util;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import java.util.Map;

public class StateUtil {
    public static MapState<String, Integer> createIntegerState(String stateName, RuntimeContext context,  int ttlDate) {
        MapStateDescriptor<String, Integer> state = new MapStateDescriptor<>(stateName, String.class, Integer.class);
        setTtl(ttlDate, state);
        return context.getMapState(state);
    }

    public static MapState<String, Map<String, Integer>> createMapState(String stateName, RuntimeContext context, int ttlDate) {
        MapStateDescriptor<String, Map<String, Integer>> state = new MapStateDescriptor<>(
                stateName, BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(String.class, Integer.class));
        setTtl(ttlDate, state);
        return context.getMapState(state);
    }

    /**
     * 设置超时时间
     * @param ttlDate
     * @param state
     */
    private static void setTtl(int ttlDate, MapStateDescriptor state) {
        StateTtlConfig stateConfig = StateTtlConfig.newBuilder(Time.days(ttlDate))
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(3000)
                .build();
        state.enableTimeToLive(stateConfig);
    }



}
