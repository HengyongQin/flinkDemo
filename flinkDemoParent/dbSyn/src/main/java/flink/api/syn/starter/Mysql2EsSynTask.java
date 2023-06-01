package flink.api.syn.starter;

import com.samur.common.pojo.MysqlRow;
import flink.api.syn.operator.EsSinkBuilder;
import flink.api.syn.operator.AssemblingAndSortProcess;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 将mysql的表数据实时同步到es中
 * 使用flink-cdc 实现全增量同步
 */
public class Mysql2EsSynTask extends AbstractMysqlTask {
    private static final String TASK_NAME = "Mysql2EsSynTask";

    public static void main(String[] args) throws Exception {
        ParameterTool param = getParam(args);
        StreamExecutionEnvironment env = createEnv(param);
        DataStreamSource<MysqlRow> source = createMysqlSource(param, env);

        source.keyBy(MysqlRow::getTableName)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                                .process(new AssemblingAndSortProcess())
                                        .addSink(EsSinkBuilder.build(param));
        env.execute(TASK_NAME);
    }
}
