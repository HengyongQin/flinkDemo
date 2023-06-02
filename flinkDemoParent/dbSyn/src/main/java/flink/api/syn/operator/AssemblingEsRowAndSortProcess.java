package flink.api.syn.operator;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
import flink.api.syn.pojo.EsRow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Map;

/**
 * esRow封装及排序
 */
public class AssemblingEsRowAndSortProcess extends ProcessWindowFunction<MysqlRow, EsRow, String, TimeWindow> {
    @Override
    public void process(String key, ProcessWindowFunction<MysqlRow, EsRow, String, TimeWindow>.Context context, Iterable<MysqlRow> elements, Collector<EsRow> out) {
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

                            String id = idBuilder.toString();
                            return new EsRow(index, id, JSON.toJSONString(after), optType, row.getTs());
                        }
                )
                .sorted(Comparator.comparing(EsRow::getPos))
                .forEach(out::collect);
    }
}
