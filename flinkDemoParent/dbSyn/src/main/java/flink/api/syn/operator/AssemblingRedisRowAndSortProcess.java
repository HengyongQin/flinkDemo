package flink.api.syn.operator;

import com.google.common.collect.Lists;
import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
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
                            String index = row.getDatabase() + "_" + row.getTableName();
                            String[] keys = row.getKeys();
                            Map<String, Object> after = row.getAfter();
                            Map<String, Object> before = row.getBefore();
                            RowOptType optType = row.getOptType();
                            StringBuilder idBuilder = new StringBuilder();

                            for (String primaryKey : keys) {
                                idBuilder.append("_").append(optType.equals(RowOptType.DELETE) ? before.get(primaryKey) : after.get(primaryKey));
                            }

                            Map<String, String> data = new HashMap<>();

                            if(after != null && !after.isEmpty()) {
                                for (Map.Entry<String, Object> entry : after.entrySet()) {
                                    data.put(entry.getKey(), String.valueOf(entry.getValue()));
                                }
                            }

                            return new RedisRow(index + idBuilder, data, row.getOptType(), row.getTs());
                        }
                )
                .sorted(Comparator.comparing(RedisRow::getPos))
                .forEach(rows::add);

        out.collect(rows.toArray(new RedisRow[0]));
    }
}
