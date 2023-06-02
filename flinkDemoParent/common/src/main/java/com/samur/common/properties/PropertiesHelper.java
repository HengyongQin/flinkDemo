package com.samur.common.properties;

import java.io.InputStream;

public class PropertiesHelper {
    public static InputStream getPropertiesResource() {
        return PropertiesHelper.class.getResourceAsStream(PropertiesConstant.PROP_PATH);
    }
}
