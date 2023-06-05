package com.samur.common.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtils {
    private static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static LocalTime dayEndTime = LocalTime.of(23, 59, 59); // 当天结束时间

    /**
     * 获取当天日期
     * @return
     */
    public static String getCurrentDate() {
        return dateFormatter.format(LocalDateTime.now());
    }

    /**
     * 获取今天结束时间戳（秒）
     * @return
     */
    public static long getTodayEndTime() {
        LocalDate today = LocalDate.now();
        Date t = Date.from(today.atTime(dayEndTime).atZone(ZoneId.systemDefault()).toInstant());
        return t.getTime()/1000;
    }

    /**
     * test
     * @param args
     */
    public static void main(String[] args) {
        System.out.println(getTodayEndTime());
    }
}
