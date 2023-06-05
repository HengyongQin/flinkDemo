package flink.api.syn.starter;

import com.samur.common.pojo.MysqlRow;
import flink.api.syn.operator.AsyncCountProcessFunction;
import flink.api.syn.operator.CountProcessFunction;
import flink.api.syn.operator.CustomRedisSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 计算产品及产品大类 浏览量，及 top 10
 */
public class ProductCountTask extends AbstractMysqlTask {
    private final static String MYSQL_LISTEN_TABLES = "flink.order_items"; // 订单明细表

    public static void main(String[] args) throws Exception {
        ParameterTool param = getParam(args, customParam());
        StreamExecutionEnvironment env = createEnv(param);

        // 同步的方式关联维表
//        DataStreamSource<MysqlRow> source = createMysqlSource(param, env);
//
//        source.keyBy(MysqlRow::getTableName)
//                .process(new CountProcessFunction())
//                .addSink(new CustomRedisSink());

        ///=================使用异步的方式==================
        DataStream<MysqlRow> mysqlStream = createMysqlSource(param, env);
        AsyncDataStream.unorderedWait(mysqlStream, new AsyncCountProcessFunction(), 1000, TimeUnit.MILLISECONDS)
                .addSink(new CustomRedisSink());
        env.execute(ProductCountTask.class.getName());
        // ============================================
    }

    /**
     * 获取自定义参数
     * @return
     */
    protected static Map<String, String> customParam() {
        HashMap<String, String> map = new HashMap<>();
        map.put("mysql_listen_tables", MYSQL_LISTEN_TABLES);
        return map;
    }
}
