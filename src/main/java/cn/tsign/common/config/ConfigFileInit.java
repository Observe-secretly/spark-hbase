package cn.tsign.common.config;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.util.ConfProperties;
import cn.tsign.common.util.HdfsUtils;

public class ConfigFileInit {

    public static void init() throws Exception {
        HdfsUtils hdfsUtils = new HdfsUtils();
        String hdfsFile = getHdfsFile(CommonConstant.CONF_TYPE_TRACK);
        if (!hdfsUtils.exist(hdfsFile)) {
            hdfsUtils.createNewHDFSFile(hdfsFile, "");
        }
        hdfsFile = getHdfsFile(CommonConstant.CONF_TYPE_BINLOG);
        if (!hdfsUtils.exist(hdfsFile)) {
            hdfsUtils.createNewHDFSFile(hdfsFile, "");
        }
        hdfsFile = getHdfsFile(CommonConstant.CONF_TYPE_ROWKEY);
        if (!hdfsUtils.exist(hdfsFile)) {
            hdfsUtils.createNewHDFSFile(hdfsFile, "");
        }
        hdfsFile = getHdfsFile(CommonConstant.CONF_TYPE_AGGREGATION);
        if (!hdfsUtils.exist(hdfsFile)) {
            hdfsUtils.createNewHDFSFile(hdfsFile, "");
        }
        hdfsFile = getHdfsFile(CommonConstant.CONF_DRUID_TASK);
        if (!hdfsUtils.exist(hdfsFile)) {
            hdfsUtils.createNewHDFSFile(hdfsFile, "");
        }
    }

    private static String getHdfsFile(String type) {
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
            case CommonConstant.CONF_DRUID_TASK:
                hdfsFile = ConfProperties.getStringValue(ConfigConstant.druid_task_conf_path);
                break;
        }
        return hdfsFile;
    }

}
