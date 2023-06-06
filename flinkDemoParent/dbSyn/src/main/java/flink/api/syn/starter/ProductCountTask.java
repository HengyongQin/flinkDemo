package flink.api.syn.starter;

import com.samur.common.pojo.MysqlRow;
import flink.api.syn.operator.*;
import flink.api.syn.pojo.RedisRow;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

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

        // ===== 以下方式任选一种 ====
        // 同步的方式关联维表
//        runAsSync(param, env);
        ///=================使用异步的方式==================///
//        runAsAsync(param, env);
        // ==================广播流========================== //
        runAsBroad(param, env);
        // ================= end ================//

        env.execute(ProductCountTask.class.getName());
    }

    /**
     * 使用广播的方式运行。将维表数据广播到 流中，适合场景：实时性要求比较高，需要刷新当天历史数据
     * @param param
     * @param env
     */
    private static void runAsBroad(ParameterTool param, StreamExecutionEnvironment env) {
        DataStream<MysqlRow> source = createMysqlSource(param, env);
        DataStreamSource<MysqlRow> productDimSource = env.fromSource(MySqlSourceBuilder.build(param, "flink.product"), waterMark(), "product_dim");
        BroadcastStream<MysqlRow> dimBroadcast = productDimSource.broadcast(
                new MapStateDescriptor<>("aaa"
                        , BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(MysqlRow.class)));

        source.connect(dimBroadcast)
                .process(new BroadCountProcessFunction()).addSink(new CustomRedisSink());
    }

    /**
     * 使用同步的方式运行
     * @param param
     * @param env
     */
    private static void runAsAsync(ParameterTool param, StreamExecutionEnvironment env) {
        DataStreamSource<MysqlRow> mysqlStream = createMysqlSource(param, env);
        AsyncDataStream.unorderedWait(mysqlStream, new AsyncCountProcessFunction(), 1000, TimeUnit.MILLISECONDS)
                .addSink(new CustomRedisSink());
    }

    /**
     * 使用异步的方式运行
     * @param param
     * @param env
     */
    private static void runAsSync(ParameterTool param, StreamExecutionEnvironment env) {
        DataStreamSource<MysqlRow> source = createMysqlSource(param, env);
        source.keyBy(MysqlRow::getTableName)
                .process(new CountProcessFunction())
                .addSink(new CustomRedisSink());
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
