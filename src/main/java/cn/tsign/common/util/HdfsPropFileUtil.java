package cn.tsign.common.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import cn.tsign.common.config.ConfigConstant;
import cn.tsign.common.constant.CommonConstant;

public class HdfsPropFileUtil {

    /**
     * 删除配置
     * 
     * @param hdfsFile
     * @param key
     * @param hdfsUtils
     * @throws IOException
     * @throws Exception
     */
    public static void removeConf(String hdfsFile, String key, HdfsUtils hdfsUtils) throws IOException, Exception {
        updateConf(hdfsFile, key, null, hdfsUtils);
    }

    /**
     * 更新配置
     * 
     * @param type
     * @param key
     * @param value
     * @throws IOException
     * @throws Exception
     */
    public static void updateConf(String hdfsFile, String key, String value, HdfsUtils hdfsUtils) throws IOException,
                                                                                                  Exception {

        String[] lines = new String(hdfsUtils.readHDFSFile(hdfsFile)).split("\n");
        Map<String, String> prop = new HashMap<>();
        if (lines != null && lines.length > 0) {
            for (String line : lines) {
                int splitIndex = line.indexOf("=");
                if (splitIndex != -1) {
                    prop.put(line.substring(0, splitIndex), line.substring(splitIndex + 1));
                }
            }
        }

        if (!StringUtil.isEmpty(value)) {// 如果value等于null则删除配置
            prop.put(key, value);// 更新配置。重复则覆盖
        } else {
            prop.remove(key);
        }

        hdfsUtils.deleteHDFSFile(hdfsFile);

        StringBuilder content = new StringBuilder();
        for (Entry<String, String> item : prop.entrySet()) {
            content.append(item.getKey().toString());
            content.append("=");
            content.append(item.getValue().toString());
            content.append("\n");
        }
        hdfsUtils.append(hdfsFile, content.toString());
    }

    public static String getHdfsFile(String type) {
        String hdfsFile = null;
        switch (type) {
            case CommonConstant.CONF_TYPE_TRACK:
                hdfsFile = ConfProperties.getStringValue(ConfigConstant.track_tablename_conf_path);
                break;
            case CommonConstant.CONF_TYPE_BINLOG:
                hdfsFile = ConfProperties.getStringValue(ConfigConstant.binlog_tablename_conf_path);
                break;
            case CommonConstant.CONF_TYPE_ROWKEY:
                hdfsFile = ConfProperties.getStringValue(ConfigConstant.rowkey_conf_path);
                break;
            case CommonConstant.CONF_TYPE_AGGREGATION:
                hdfsFile = ConfProperties.getStringValue(ConfigConstant.agg_conf_path);
                break;
            case CommonConstant.CONF_ALARMS:
                hdfsFile = ConfProperties.getStringValue(ConfigConstant.alarms_conf_path);
                break;
            case CommonConstant.CONF_ALARM_NOTIFICATION:
                hdfsFile = ConfProperties.getStringValue(ConfigConstant.alarms_notification_path);
                break;

        }
        return hdfsFile;
    }

}
