package flink.api.syn.starter.cep;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 连续3次登入失败监控
 */
public class LoginFailCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
        KeyedStream<JSONObject, String> stream = env.fromElements(
                        "{\"loginId\":11111,\"loginTime\":1645177352000,\"loginStatus\":1,\"userName\":\"aaron\"}",
                        "{\"loginId\":11112,\"loginTime\":1645177353000,\"loginStatus\":1,\"userName\":\"aaron\"}",
                        "{\"loginId\":11113,\"loginTime\":1645177354000,\"loginStatus\":0,\"userName\":\"aaron\"}",
                        "{\"loginId\":11116,\"loginTime\":1645177355000,\"loginStatus\":1,\"userName\":\"aaron\"}",
                        "{\"loginId\":11117,\"loginTime\":1645177356000,\"loginStatus\":1,\"userName\":\"aaron\"}",
                        "{\"loginId\":11118,\"loginTime\":1645177357000,\"loginStatus\":1,\"userName\":\"aaron\"}",
                        "{\"loginId\":11119,\"loginTime\":1645177358000,\"loginStatus\":1,\"userName\":\"aaron\"}",
                        "{\"loginId\":11120,\"loginTime\":1645177359000,\"loginStatus\":0,\"userName\":\"aaron\"}",
                        "{\"loginId\":11121,\"loginTime\":1645177360000,\"loginStatus\":1,\"userName\":\"aaron\"}",
                        "{\"loginId\":11122,\"loginTime\":1645177361000,\"loginStatus\":1,\"userName\":\"aaron\"}",
                        "{\"loginId\":11123,\"loginTime\":1645177362000,\"loginStatus\":1,\"userName\":\"aaron\"}"
                )
                .map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("loginTime");
                            }
                        }
                ))
                .keyBy(e -> e.getString("userName"));

        // 连续3次
        Pattern<JSONObject, JSONObject> pattern1 = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getInteger("loginStatus").equals(1);
                    }
                }).times(3).consecutive().within(Time.seconds(10));

        // 不连续3次
        Pattern<JSONObject, JSONObject> pattern2 = Pattern.<JSONObject>begin("start").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value, Context<JSONObject> context) throws Exception {
                return value.getInteger("loginStatus").equals(1);
            }
        }).times(3).within(Time.seconds(10));

        CEP.pattern(stream, pattern1).process(new PatternProcessFunction<JSONObject, String>() {
            @Override
            public void processMatch(Map<String, List<JSONObject>> map, Context context, Collector<String> out) throws Exception {
                List<JSONObject> list = map.get("begin");
                if(list == null) return;
                list.forEach(e -> out.collect(e.toJSONString()));
            }
        }).print("111");

//        CEP.pattern(stream, pattern2).process(new PatternProcessFunction<JSONObject, String>() {
//            @Override
//            public void processMatch(Map<String, List<JSONObject>> map, Context context, Collector<String> out) throws Exception {
//                List<JSONObject> list = map.get("begin");
//                if(list == null) return;
//                list.forEach(e -> out.collect(e.toJSONString()));
//            }
//        }).print("result2");

        env.execute();
    }
}
