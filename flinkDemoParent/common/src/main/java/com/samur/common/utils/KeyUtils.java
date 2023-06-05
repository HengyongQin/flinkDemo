package com.samur.common.utils;


import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

/**
 * 专门用来生成 key
 */
public class KeyUtils {
    /**
     * 创建redis的key
     * @param database
     * @param tableName
     * @param keys
     * @param data
     * @return
     */
    public static String createRedisKey(String database, String tableName, String[] keys, Map<String, Object> data) {
        StringBuilder builder = getKeyHeader(database, tableName);

        Arrays.stream(keys)
                .sorted(Comparator.comparing(String::hashCode))
                .forEach(e -> builder.append(data.get(e)).append("_"));

        return builder.toString();
    }

    /**
     * 创建 redis的key
     * @param database
     * @param tableName
     * @param keyValue
     * @return
     */
    public static String createRedisKey(String database, String tableName, String keyValue) {
        return getKeyHeader(database, tableName)
                .append(keyValue).append("_").toString();
    }

    private static StringBuilder getKeyHeader(String database, String tableName) {
        StringBuilder builder = new StringBuilder();
        return builder.append(database)
                .append("_")
                .append(tableName)
                .append("_");
    }
}
