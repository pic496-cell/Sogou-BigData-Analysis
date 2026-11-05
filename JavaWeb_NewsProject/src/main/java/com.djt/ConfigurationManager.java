package com.djt;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
    private static Properties prop = new Properties();

    // 静态代码块，类加载时读取一次配置文件
    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }
}