package org.cloud.utils;

public class Utils {
    public static String logWithTime(String message) {
        return "[" + TimeUtils.getCurrentTime() + "] " + message;
    }
}
