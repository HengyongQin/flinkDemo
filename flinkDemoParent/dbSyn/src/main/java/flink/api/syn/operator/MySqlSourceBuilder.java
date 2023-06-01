package flink.api.syn.operator;

import com.samur.common.pojo.MysqlRow;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import flink.api.syn.constant.PropertiesConstant;
import flink.api.syn.serial.MysqlBinlogSerialize;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;

/**
 * 创建mysql数据源
 * @return
 */
public class MySqlSourceBuilder {
    private MySqlSourceBuilder() {}

    /**
     * 创建mysql数据源
     * @param param
     * @return
     */
    public static MySqlSource<MysqlRow> build(ParameterTool param) {
        String listenTables = param.get(PropertiesConstant.MYSQL_ES_LISTEN_TABLES);
        return MySqlSource.<MysqlRow>builder()
                .hostname(param.get(PropertiesConstant.MYSQL_HOST))
                .port(param.getInt(PropertiesConstant.MYSQL_PORT))
                .databaseList(getDatabaseList(listenTables))
                .tableList(getTableList(listenTables))
                .username(param.get(PropertiesConstant.MYSQL_USERNAME))
                .password(param.get(PropertiesConstant.MYSQL_PASS))
                .includeSchemaChanges(false)  // 不需要schema信息
                .deserializer(new MysqlBinlogSerialize())
                .scanNewlyAddedTableEnabled(true)
                .build();
    }

    /**
     * 获取监控库名
     * @param tableList
     * @return
     */
    private static String[] getDatabaseList(String tableList) {
        String[] tables = tableList.split(PropertiesConstant.PARAM_SPLIT_SYMBOL);
        return Arrays.stream(tables)
                .map(e -> e.split(PropertiesConstant.TABLE_SPLIT_SYMBOL)[0].trim())
                .distinct()
                .toArray(String[]::new);
    }

    /**
     * 获取监控表名
     * @param tableList
     * @return
     */
    private static String[] getTableList(String tableList) {
        return tableList
                .replace(" ", "")
                .split(PropertiesConstant.PARAM_SPLIT_SYMBOL);
    }
}
