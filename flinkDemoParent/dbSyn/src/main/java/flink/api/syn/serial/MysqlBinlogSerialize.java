package flink.api.syn.serial;

import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MysqlBinlogSerialize implements DebeziumDeserializationSchema<MysqlRow> {
    @Override
    public void deserialize(SourceRecord record, Collector<MysqlRow> out) {
        String topic = record.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String tableName = split[2];
        Struct struct = (Struct) record.value();
        MysqlRow row = new MysqlRow(getOptType(struct),
                parseStruct2Map(struct.getStruct("before")),
                parseStruct2Map(struct.getStruct("after")),
                struct.getInt64("ts_ms"),
                database,
                tableName,
                getKeys(record)
        );

        out.collect(row);
    }

    /**
     * 获取主键
     * @param record
     * @return
     */
    private String[] getKeys(SourceRecord record) {
        return record.keySchema().fields()
                .stream()
                .map(Field::name)
                .toArray(String[]::new);
    }

    @Override
    public TypeInformation<MysqlRow> getProducedType() {
        return TypeInformation.of(new TypeHint<MysqlRow>() {});
    }

    /**
     * 将struct雷子那个转化成map
     * @param struct
     * @return
     */
    private Map<String, Object> parseStruct2Map(Struct struct) {
        if(struct == null) {
            return null;
        }

        List<Field> fields = struct.schema().fields();
        Map<String, Object> map = new HashMap<>(fields.size());
        fields.forEach(column -> map.put(column.name(), struct.get(column)));
        return map;
    }

    /**
     * 操作类型转换
     * @param struct
     * @return
     */
    private RowOptType getOptType(Struct struct) {
        String op = struct.getString("op");
        RowOptType type = null;

        switch (op) {
            case "r":
            case "c":
                type = RowOptType.INSERT;
                break;
            case "u":
                type = RowOptType.UPDATE;
                break;
            case "d":
                type = RowOptType.DELETE;
                break;
            default:
                throw new RuntimeException(String.format("不支持操作类型：%s", op));
        }

        return type;
    }
}
