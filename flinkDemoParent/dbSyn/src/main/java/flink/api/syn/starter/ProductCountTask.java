package flink.api.syn.starter;

import com.samur.common.pojo.MysqlRow;
import flink.api.syn.operator.CountProcessFunction;
import flink.api.syn.operator.CustomRedisSink;
import flink.api.syn.pojo.RedisRow;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * 计算产品及产品大类 浏览量，及 top 10
 */
public class ProductCountTask extends AbstractMysqlTask {
    public static void main(String[] args) throws IOException {
        ParameterTool param = getParam(args);
        StreamExecutionEnvironment env = createEnv(param);
        DataStreamSource<MysqlRow> source = createMysqlSource(param, env);

        source.process(new CountProcessFunction())
                .addSink(new CustomRedisSink());
    }
}
