package flink.api.syn.operator;

import com.samur.common.pojo.MysqlRow;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import flink.api.syn.serial.MysqlBinlogSerialize;

/**
 * 创建mysql数据源
 * @return
 */
public class MySqlSourceBuilder {
    private MySqlSourceBuilder() {}

    public static MySqlSource<MysqlRow> build() {
        return MySqlSource.<MysqlRow>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("flink")
                .tableList("flink.stu")
                .username("root")
                .password("root")
                .includeSchemaChanges(false)  // 不需要schema信息
                .deserializer(new MysqlBinlogSerialize())
                .scanNewlyAddedTableEnabled(true)
                .build();
    }
}
