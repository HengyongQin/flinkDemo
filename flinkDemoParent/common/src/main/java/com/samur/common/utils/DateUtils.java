package com.samur.common.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtils {
    private static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * 获取当天日期
     * @return
     */
    public static String getCurrentDate() {
        return dateFormatter.format(LocalDateTime.now());
    }
}
