package com.samur.common.properties;

public class PropertiesConstant {

    /**
     * 配置文件路径
     */
    public static final String PROP_PATH = "/application.properties";

    /**
     * 是否是开发环境
     */
    public static final String IS_DEV = "is.dev";

    /**
     * checkpoint 路径
     */
    public static final String FLINK_CHECKPOINT_PATH = "flink.checkpoint.path";

    /**
     * mysql 配置
     */
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";
    public static final String MYSQL_PASS = "mysql.pass";

    /**
     * mysql 同步到es 的表
     */
    public static final String MYSQL_ES_LISTEN_TABLES = "mysql.es.listen.tables";

    /**
     * es配置
     */
    public static final String ES_HOST = "es.host";
    public static final String ES_PORT = "es.port";
    public static final String ES_USERNAME = "es.username";
    public static final String ES_PASS = "es.pass";

    /**
     * 参数切割符
     */
    public static final String PARAM_SPLIT_SYMBOL = ",";
    public static final String TABLE_SPLIT_SYMBOL = "\\.";

    /**
     * redis 配置
     */
    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";

}
