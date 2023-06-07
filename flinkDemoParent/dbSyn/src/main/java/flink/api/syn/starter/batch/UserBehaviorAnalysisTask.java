package flink.api.syn.starter.batch;

import flink.api.syn.pojo.ItemCount;
import flink.api.syn.pojo.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.TreeSet;

/**
 * 用户行为分析
 */
public class UserBehaviorAnalysisTask {
    private static final int topSize = 2;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStream<UserBehavior> stream = getStream(env);

        stream.filter(e -> UserBehavior.BehaviorType.PV.equals(e.getBehavior()))  // 过滤出浏览行为
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.days(1), Time.minutes(5)))
                .aggregate(new AggregateFunction<UserBehavior, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(UserBehavior value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                }, (WindowFunction<Integer, ItemCount, Long, TimeWindow>) (key, window, input, out) ->
                        out.collect(new ItemCount(key, input.iterator().next(), window.getEnd())))
                .returns(TypeInformation.of(ItemCount.class))
                .keyBy(ItemCount::getTs)
                .process(new ItemCountProcess()).setParallelism(1)
                .print().setParallelism(1);


        env.execute(UserBehaviorAnalysisTask.class.getName());
    }

    private static class ItemCountProcess extends KeyedProcessFunction<Long, ItemCount, String> {
        private ListState<ItemCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("aa", ItemCount.class));
        }

        @Override
        public void processElement(ItemCount item, KeyedProcessFunction<Long, ItemCount, String>.Context ctx, Collector<String> out) throws Exception {
            listState.add(item);
            ctx.timerService().registerProcessingTimeTimer(item.getTs());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            TreeSet<ItemCount> itemCounts = new TreeSet<>(ItemCount::compareTo);
            listState.get().forEach(itemCounts::add);
            listState.clear();
            StringBuilder builder = new StringBuilder(Thread.currentThread().getId() + "统计窗口结束时间：")
                    .append(new Timestamp(timestamp));
            int index = 1;

            for (ItemCount itemCount : itemCounts) {
                builder.append(String.format(" NO %s: 商品id=%s ，浏览量：%s, 创建线程： %s", index, itemCount.getItemId(), itemCount.getCount(), itemCount.getThreadId()));

                if(++index > topSize) {
                    break;
                }
            }

            out.collect(builder.toString());
        }
    }

    private static StreamExecutionEnvironment getEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
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
