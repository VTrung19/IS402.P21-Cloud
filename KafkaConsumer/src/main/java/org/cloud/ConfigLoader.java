package org.cloud;

import org.apache.commons.configuration.PropertiesConfiguration;

public class ConfigLoader {

    private static final PropertiesConfiguration config;

    static {
        try {
            config = new PropertiesConfiguration("application.properties");
        } catch (Exception e) {
            throw new RuntimeException("Can't load configuration with error", e);
        }
    }

    public static String get(String key) {
        return config.getString(key);
    }
}
