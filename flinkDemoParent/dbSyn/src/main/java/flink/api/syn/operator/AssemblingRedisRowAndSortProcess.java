package flink.api.syn.operator;

import com.google.common.collect.Lists;
import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
import com.samur.common.utils.KeyUtils;
import flink.api.syn.pojo.RedisRow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * redisRow 封装及排序
 */
public class AssemblingRedisRowAndSortProcess extends ProcessWindowFunction<MysqlRow, RedisRow[], String, TimeWindow> {
    @Override
    public void process(String key, ProcessWindowFunction<MysqlRow, RedisRow[], String, TimeWindow>.Context context, Iterable<MysqlRow> elements, Collector<RedisRow[]> out) {
        List<RedisRow> rows = new ArrayList<>();

        Lists.newArrayList(elements).stream()
                .map(
                        row -> {
                            Map<String, Object> after = row.getAfter();
                            Map<String, Object> before = row.getBefore();
                            Map<String, String> data = new HashMap<>();

                            if(after != null && !after.isEmpty()) {
                                for (Map.Entry<String, Object> entry : after.entrySet()) {
                                    data.put(entry.getKey(), String.valueOf(entry.getValue()));
                                }
                            }

                            String redisKey = KeyUtils.createRedisKey(row.getDatabase(), row.getTableName(), row.getKeys()
                                    , before == null || before.isEmpty() ? after : before);
                            return new RedisRow(redisKey, data, row.getOptType(), row.getTs());
                        }
                )
                .sorted(Comparator.comparing(RedisRow::getPos))
                .forEach(rows::add);

        out.collect(rows.toArray(new RedisRow[0]));
    }
}
