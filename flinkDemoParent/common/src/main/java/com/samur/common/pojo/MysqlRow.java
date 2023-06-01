package com.samur.common.pojo;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

public class MysqlRow implements Serializable {
    private static final long serialVersionUID = 1L;

    private MysqlRow() {}

    public MysqlRow(RowOptType optType, Map<String, Object> before, Map<String, Object> after, long ts, String database, String tableName, String[] keys) {
        this.optType = optType;
        this.before = before;
        this.after = after;
        this.ts = ts;
        this.database = database;
        this.tableName = tableName;
        this.keys = keys;
    }

    @Getter
    private RowOptType optType;

    @Getter
    private Map<String, Object> before;

    @Getter
    private Map<String, Object> after;

    @Getter
    private long ts;

    @Getter
    private String database;

    @Getter
    private String tableName;

    @Getter
    private String[] keys;

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }
}
