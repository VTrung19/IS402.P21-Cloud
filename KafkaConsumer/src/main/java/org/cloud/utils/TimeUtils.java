package org.cloud.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.cloud.Constant.DATE_STRING_FORMAT;

public class TimeUtils {

    public static String getCurrentTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATE_STRING_FORMAT));
    }
}
