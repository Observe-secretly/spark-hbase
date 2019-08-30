package cn.tsign.spark.broadcast.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.alibaba.fastjson.JSON;

import cn.tsign.common.constant.SinkEnum;
import cn.tsign.common.druid.util.DruidTaskInfo;
import cn.tsign.common.util.StringUtil;

public class CoreConfig implements Serializable {

    private static final long            serialVersionUID        = -2931408206632407021L;

    private Map<String, String>          trackTableNameSettings  = new HashMap<>();

    private Map<String, String>          rowkeySettings          = new HashMap<>();

    private Map<String, String>          binlogTableNameSettings = new HashMap<>();

    private Map<String, DruidTaskInfo>   druidTaskInfoConfig     = new HashMap<>();

    /**
     * Key:要聚合的表名称</br>
     * Value:聚合实体
     */
    private Map<String, List<AggConfig>> aggSettings             = new HashMap<>();

    public void putBinlogTableNameSetting(String k, String v) {
        binlogTableNameSettings.put(k, v);
    }

    public void putTrackTableNameSetting(String k, String v) {
        trackTableNameSettings.put(k, v);
    }

    public void putRowkeySetting(String k, String v) {
        rowkeySettings.put(k, v);
    }

    public void putAggSetting(String k, List<AggConfig> v) {
        aggSettings.put(k, v);
    }

    public void putDruidTaskInfoConfig(String k, DruidTaskInfo v) {
        druidTaskInfoConfig.put(k, v);
    }

    public String getTrackTableNameSetting(String k) {
        String v = trackTableNameSettings.get(k);
        if (!StringUtil.isEmpty(v)) {
            return v;
        }
        return null;
    }

    public String[] getRowkeySetting(String k) {
        String v = rowkeySettings.get(k);
        if (!StringUtil.isEmpty(v)) {
            return v.split(",");
        }
        return null;
    }

    public String getBinlogTableNameSetting(String k) {
        String v = binlogTableNameSettings.get(k);
        if (!StringUtil.isEmpty(v)) {
            return v;
        }
        return null;
    }

    public List<AggConfig> getAggSetting(String k) {
        List<AggConfig> v = aggSettings.get(k);
        if (v != null) {
            return v;
        }
        return null;
    }

    public DruidTaskInfo getDruidTaskInfo(String k) {
        DruidTaskInfo v = druidTaskInfoConfig.get(k);
        if (v != null) {
            return v;
        }
        return null;
    }

    public Map<String, String> getTrackTableNameSettings() {
        return trackTableNameSettings;
    }

    public Map<String, String> getRowkeySettings() {
        return rowkeySettings;
    }

    public Map<String, String> getBinlogTableNameSettings() {
        return binlogTableNameSettings;
    }

    public Map<String, List<AggConfig>> getAggSettings() {
        return aggSettings;
    }

    public Map<String, DruidTaskInfo> getDruidTaskInfoConfig() {
        return druidTaskInfoConfig;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("\n trackTableNameSettings-->");
        result.append(JSON.toJSONString(trackTableNameSettings));
        result.append("\n rowkeySettings-->");
        result.append(JSON.toJSONString(rowkeySettings));
        result.append("\n binlogTableNameSettings-->");
        result.append(JSON.toJSONString(binlogTableNameSettings));
        result.append("\n aggSettings-->");
        result.append(JSON.toJSONString(aggSettings));
        result.append("\n DruidTaskInfoConfig-->");
        result.append(JSON.toJSONString(druidTaskInfoConfig));
        return result.toString();
    }

    /**
     * 聚合配置实体
     * 
     * @author limin Jul 2, 2019 11:50:24 AM
     */
    public static class AggConfig implements Serializable {

        private static final long serialVersionUID = -7724822499067194444L;

        private String            uuid             = UUID.randomUUID().toString();

        /**
         * 聚合之后的数据源名称<br>
         * 如果是sink是Hbase，这里代表hbase表名，你需要提前建立好这张表<br>
         * 如果是sink是druid,这里代表数据源名称
         */
        private String            sourceName;

        /**
         * 定义rowkey的生成规则
         */
        private String            group;

        /**
         * 聚合内容
         */
        private String            agg;

        /**
         * hbase或者druid
         */
        private SinkEnum          sink;

        public String getUuid() {
            return uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getSourceName() {
            return sourceName;
        }

        public void setSourceName(String sourceName) {
            this.sourceName = sourceName;
        }

        public SinkEnum getSink() {
            return sink;
        }

        public void setSink(SinkEnum sink) {
            this.sink = sink;
        }

        public String getGroup() {
            return group;
        }

        public String[] getGroupFields() {
            String[] groupFields = getGroup().trim().replaceAll(" ", "").split(",");
            return groupFields;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getAgg() {
            return agg;
        }

        public String[] getAggFields() {
            String[] aggFields = getAgg().trim().replaceAll(" ", "").split(",");
            return aggFields;
        }

        public void setAgg(String agg) {
            this.agg = agg;
        }

    }

}
