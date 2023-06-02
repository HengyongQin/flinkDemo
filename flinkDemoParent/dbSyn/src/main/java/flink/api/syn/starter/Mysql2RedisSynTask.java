package flink.api.syn.starter;

import com.samur.common.pojo.MysqlRow;
import flink.api.syn.operator.AssemblingRedisRowAndSortProcess;
import flink.api.syn.operator.CustomRedisSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * mysql 数据同步到 redis
 */
public class Mysql2RedisSynTask extends AbstractMysqlTask {
    private static final String TASK_NAME = "Mysql2RedisSynTask";

    public static void main(String[] args) throws Exception {
        ParameterTool param = getParam(args);
        StreamExecutionEnvironment env = createEnv(param);
        DataStreamSource<MysqlRow> source = createMysqlSource(param, env);

        source.keyBy(MysqlRow::getTableName)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new AssemblingRedisRowAndSortProcess())
                .addSink(new CustomRedisSink());

        env.execute(TASK_NAME);
    }
}
