package flink.api.syn.starter.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TreeSet;

public class UrlViewCountTask {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss");
    private static final Time WINDOW_LENGTH = Time.minutes(10);
    private static final Time REFRESH_INTERVAL = Time.seconds(5);
    private static final int topSize = 5;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStream<ApacheLogEvent> source = getSource(env);
        source.keyBy(ApacheLogEvent::getUrl)
                .window(SlidingEventTimeWindows.of(WINDOW_LENGTH, REFRESH_INTERVAL))
                .reduce(new AggregationFunction<ApacheLogEvent>() {
                    @Override
                    public ApacheLogEvent reduce(ApacheLogEvent value1, ApacheLogEvent value2) throws Exception {
                        return new ApacheLogEvent(value1.url, value1.ts, value1.count + value2.count);
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(REFRESH_INTERVAL))
                .process(new ProcessAllWindowFunction<ApacheLogEvent, String, TimeWindow>() {

                    @Override
                    public void process(ProcessAllWindowFunction<ApacheLogEvent, String, TimeWindow>.Context context, Iterable<ApacheLogEvent> elements, Collector<String> out) throws Exception {
                        TreeSet<ApacheLogEvent> treeSet = new TreeSet<>();
                        elements.forEach(treeSet::add);

                        StringBuilder builder = new StringBuilder("=============== 统计窗口结束时间：")
                                .append(new Timestamp(context.window().getEnd()))
                                .append(" =============== \n");
                        int index = 1;

                        for (ApacheLogEvent event : treeSet) {
                            builder.append(String.format(" NO %s: URL=%s ，浏览量：%s", index, event.getUrl(), event.getCount()))
                                    .append("\n");

                            if(++index > topSize) {
                                break;
                            }
                        }

                        out.collect(builder.toString());
                    }
                }).print().setParallelism(1);

        env.execute(UrlViewCountTask.class.getName());
    }

    private static StreamExecutionEnvironment getEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        return env;
    }

    private static DataStream<ApacheLogEvent> getSource(StreamExecutionEnvironment env) {
        return env.readTextFile("D:\\project\\flinkDemo\\flinkDemoParent\\dbSyn\\src\\main\\resources\\apache.log")
                .map(s -> {
                    String[] arr = s.split(" ");
                    long ts = LocalDateTime.parse(arr[3], formatter).toInstant(ZoneOffset.UTC).toEpochMilli();
                    return new ApacheLogEvent(arr[6], ts, 1);
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.ts;
                    }
                }));

    }

    @Data
    @AllArgsConstructor
    private static class ApacheLogEvent implements Comparable<ApacheLogEvent> {
        private String url;

        private long ts;

        private int count;

        @Override
        public int compareTo(ApacheLogEvent n) {
            return count == n.count ? url.compareTo(n.url) : Integer.compare(n.count, count);
        }
    }
}
