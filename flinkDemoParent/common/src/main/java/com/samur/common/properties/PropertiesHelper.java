package com.samur.common.properties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置文件管理
 */
public class PropertiesHelper {
    private static Properties prop = new Properties();

    static {
        try {
            prop.load(PropertiesHelper.class.getResourceAsStream(PropertiesConstant.PROP_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取配置文件流
     * @return
     */
    public static InputStream getPropertiesResource() {
        return PropertiesHelper.class.getResourceAsStream(PropertiesConstant.PROP_PATH);
    }

    /**
     * 配置文件读取
     * @param key
     * @return
     */
    public static String getValue(String key) {
        return prop.getProperty(key);
    }

    /**
     * 配置文件读取
     * @param key
     * @return
     */
    public static int getInt(String key) {
        return Integer.parseInt(getValue(key));
    }
}
