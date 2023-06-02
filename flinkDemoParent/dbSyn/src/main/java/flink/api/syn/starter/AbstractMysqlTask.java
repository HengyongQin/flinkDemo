package flink.api.syn.starter;

import com.samur.common.pojo.MysqlRow;
import com.samur.common.properties.PropertiesConstant;
import com.samur.common.properties.PropertiesHelper;
import flink.api.syn.operator.MySqlSourceBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import java.io.IOException;

/**
 * mysql数据源任务 抽象
 */
public abstract class AbstractMysqlTask {
    private static final String MYSQL_SOURCE_NAME = "mysql_source_name";

    /**
     * 创建mysql数据源
     * @param param
     * @param env
     * @return
     */
    protected static DataStreamSource<MysqlRow> createMysqlSource(ParameterTool param, StreamExecutionEnvironment env) {
        return env.fromSource(MySqlSourceBuilder.build(param), waterMark(), MYSQL_SOURCE_NAME);
    }

    /**
     * 创建执行环境
     * @param param
     * @return
     */
    protected static StreamExecutionEnvironment createEnv(ParameterTool param) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        if(!param.getBoolean(PropertiesConstant.IS_DEV)) { // 开发环境不需要checkpoint
            setCheckPoint(env, param);
        }

        return env;
    }

    /**
     * 获取系统参数
     * @param args
     * @return
     * @throws IOException
     */
    protected static ParameterTool getParam(String[] args) throws IOException {
        return ParameterTool.fromPropertiesFile(PropertiesHelper.getPropertiesResource())
                .mergeWith(ParameterTool.fromArgs(args));
    }

    /**
     * 设置 check point
     * @param env
     * @param param
     */
    private static void setCheckPoint(StreamExecutionEnvironment env, ParameterTool param) {
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // 增量checkpoint
        env.enableCheckpointing(60*1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(2); // 最大 checkpoint 保留数量
        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000); // 两次checkpoint最小间隔
        checkpointConfig.setCheckpointTimeout(600 * 1000);// check point 超时时间
        checkpointConfig.setCheckpointStorage(param.get(PropertiesConstant.FLINK_CHECKPOINT_PATH));
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
