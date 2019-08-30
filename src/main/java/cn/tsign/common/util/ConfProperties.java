package cn.tsign.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import cn.tsign.common.config.ConfDev;
import cn.tsign.common.config.ConfRelease;
import cn.tsign.common.config.ConfTest;
import cn.tsign.common.config.ConfigConstant;
import cn.tsign.common.config.Env;

/**
 * 工程启动时加载properties文件
 * 
 * @author limin 2016年3月24日 下午4:53:35
 */
public class ConfProperties {

    public static String getStringValue(ConfigConstant key) {
        String env = Env.getEnvironment();

        if (env.trim().toLowerCase().equals("dev")) {
            return ConfDev.getConf().get(key);
        } else if (env.trim().toLowerCase().equals("test")) {
            return ConfTest.getConf().get(key);
        } else if (env.trim().toLowerCase().equals("release")) {
            return ConfRelease.getConf().get(key);
        }

        return null;
    }

    public static Integer getIntegerValue(ConfigConstant key) {
        String stringValue = getStringValue(key);
        if (StringUtil.isEmpty(stringValue)) {
            return null;
        }
        return Integer.parseInt(stringValue);
    }

    public static Long getLongValue(ConfigConstant key) {
        String stringValue = getStringValue(key);
        if (StringUtil.isEmpty(stringValue)) {
            return null;
        }
        return Long.parseLong(stringValue);
    }

    public static Boolean getBooleanValue(ConfigConstant key) {
        return Boolean.parseBoolean(getStringValue(key));
    }

    public static Date getDateValue(SimpleDateFormat format, ConfigConstant key) throws ParseException {
        return format.parse(getStringValue(key));
    }

}
