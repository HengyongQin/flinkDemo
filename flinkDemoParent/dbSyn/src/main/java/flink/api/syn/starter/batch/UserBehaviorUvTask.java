package flink.api.syn.starter.batch;

import flink.api.syn.pojo.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * 用户行为分析 计算浏览量top3的产品id
 */
public class UserBehaviorUvTask {
    private static final int topSize = 3;
    private static final Time WINDOW_LENGTH = Time.hours(1);  // 窗口长度
    private static final Time REFRESH_INTERVAL = Time.minutes(5); // 数据刷新时间

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStream<UserBehavior> stream = getStream(env);

        stream.filter(e -> UserBehavior.BehaviorType.PV.equals(e.getBehavior()))  // 过滤出浏览行为
                .windowAll(TumblingEventTimeWindows.of(WINDOW_LENGTH))
                .trigger(ContinuousEventTimeTrigger.of(REFRESH_INTERVAL))
                .process(new ProcessAllWindowFunction<UserBehavior, String, TimeWindow>() {

                    @Override
                    public void process(ProcessAllWindowFunction<UserBehavior, String, TimeWindow>.Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
                        HashSet<String> set = new HashSet<>();
                        elements.forEach(e -> set.add(String.valueOf(e.getUserId())));

                        StringBuilder builder = new StringBuilder("=============== 统计窗口结束时间：")
                                .append(new Timestamp(context.window().getEnd())).append(" =============== 今日独立访问人数:").append(set.size())
                                .append("============");
                        out.collect(builder.toString());
                    }
                })
                .print().setParallelism(1);


        env.execute(UserBehaviorUvTask.class.getName());
    }


    private static StreamExecutionEnvironment getEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;

    }

    private static DataStream<UserBehavior> getStream(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("D:\\project\\flinkDemo\\flinkDemoParent\\dbSyn\\src\\main\\resources\\UserBehavior.csv");
        return source.map(e -> {
            String[] arr = e.split(",");
            return new UserBehavior(Long.parseLong(arr[0]), Long.parseLong(arr[1])
                    , Integer.parseInt(arr[2]), arr[3], Long.parseLong(arr[4]));
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.milliseconds(500)) {
            @Override
            public long extractTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000;
            }
        }));
    }
}
