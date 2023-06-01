package flink.api.syn.starter;

import com.alibaba.fastjson.JSON;
import com.samur.common.pojo.MysqlRow;
import com.samur.common.pojo.RowOptType;
import flink.api.syn.constant.PropertiesConstant;
import flink.api.syn.operator.EsSinkBuilder;
import flink.api.syn.operator.MySqlSourceBuilder;
import flink.api.syn.operator.AssemblingAndSortProcess;
import flink.api.syn.pojo.EsRow;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import java.io.IOException;

/**
 * 将mysql的表数据实时同步到es中
 * 使用flink-cdc 实现全增量同步
 */
public class Mysql2EsSynTask {
    private static final String MYSQL_SOURCE_NAME = "mysql_source_name";
    private static final String TASK_NAME = "Mysql2EsSynTask";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        ParameterTool param = getParam(args);

        if(!param.getBoolean(PropertiesConstant.IS_DEV)) { // 开发环境不需要checkpoint
            setCheckPoint(env);
        }

        DataStreamSource<MysqlRow> source = env.fromSource(MySqlSourceBuilder.build(), waterMark(), MYSQL_SOURCE_NAME);
        source.keyBy(MysqlRow::getTableName)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                                .process(new AssemblingAndSortProcess())
                                        .addSink(EsSinkBuilder.build());
        env.execute(TASK_NAME);
    }

    /**
     * 获取系统参数
     * @param args
     * @return
     * @throws IOException
     */
    private static ParameterTool getParam(String[] args) throws IOException {
        return ParameterTool.fromPropertiesFile(Mysql2EsSynTask.class.getResourceAsStream(PropertiesConstant.PROP_PATH))
                .mergeWith(ParameterTool.fromArgs(args));
    }

    /**
     * 设置 check point
     * @param env
     */
    private static void setCheckPoint(StreamExecutionEnvironment env) {
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // 增量checkpoint
        env.enableCheckpointing(60*1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(2); // 最大 checkpoint 保留数量
        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000); // 两次checkpoint最小间隔
        checkpointConfig.setCheckpointTimeout(600 * 1000);// check point 超时时间
        checkpointConfig.setCheckpointStorage("file:///cygdrive/d/soft/bigdata/flink/flink-1.13.6/flink-checkpoints/dbSyn");
        // 设置作业失败时，保留checkpoint
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    /**
     * 水印
     * @return
     */
    private static AssignerWithPeriodicWatermarksAdapter.Strategy<MysqlRow> waterMark() {
        return new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<MysqlRow>(Time.milliseconds(500)) {
                    @Override
                    public long extractTimestamp(MysqlRow e) {
                        return e.getTs();
                    }
                });
    }
}
