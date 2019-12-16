package cn.tsign.entity;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.hbase.ColumnFamily;
import cn.tsign.common.util.StatisTimeUtil;

public class TrackEntity extends StoreAbstract {

    private static final long   serialVersionUID = -3751601249154834548L;

    private Map<String, Object> lib;

    private Map<String, Object> properties;

    private Map<String, Object> ext_properties;

    private String              distinct_id;

    private long                time;

    private String              type;

    private String              event;

    private String              cid;

    private String              cname;

    private String              _nocache;

    public Map<String, Object> getLib() {
        return lib;
    }

    public void setLib(Map<String, Object> lib) {
        this.lib = lib;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Map<String, Object> getExt_properties() {
        return ext_properties;
    }

    public void setExt_properties(Map<String, Object> ext_properties) {
        this.ext_properties = ext_properties;
    }

    public String getDistinct_id() {
        return distinct_id;
    }

    public void setDistinct_id(String distinct_id) {
        this.distinct_id = distinct_id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getCname() {
        return cname;
    }

    public void setCname(String cname) {
        this.cname = cname;
    }

    public String get_nocache() {
        return _nocache;
    }

    public void set_nocache(String _nocache) {
        this._nocache = _nocache;
    }

    @Override
    public List<ColumnFamily> toHbase() {
        ColumnFamily columnFamily = new ColumnFamily(CommonConstant.COLUMN_FAMILY_NAME);
        Map<String, Object> map = trackToMap();
        for (Entry<String, Object> entry : map.entrySet()) {
            columnFamily.add(entry.getKey(), entry.getValue() == null ? Bytes.toBytes("") : getValue(entry.getValue()));
        }
        // 追加时间纬度属性，用来统计用
        if (getTime() > 0l) {
            StatisTimeUtil statisTimeUtil = new StatisTimeUtil(getTime());
            columnFamily.add("statisYear", Bytes.toBytes(statisTimeUtil.get().getStatisYear()));
            columnFamily.add("statisMonth", Bytes.toBytes(statisTimeUtil.get().getStatisMonth()));
            columnFamily.add("statisDay", Bytes.toBytes(statisTimeUtil.get().getStatisDay()));
            columnFamily.add("statisHour", Bytes.toBytes(statisTimeUtil.get().getStatisHour()));
        }

        return Arrays.asList(columnFamily);
    }

    public Map<String, Object> trackToMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("distinct_id", this.getDistinct_id());
        map.put("time", this.getTime());
        map.put("type", this.getType());
        map.put("event", this.getEvent());
        map.put("cid", this.getCid());
        map.put("cname", this.getCname());
        map.put("_nocache", this.get_nocache());

        for (Entry<String, Object> item : this.getExt_properties().entrySet()) {
            map.put("ext." + item.getKey().toLowerCase(), item.getValue());
        }

        for (Entry<String, Object> item : this.getProperties().entrySet()) {
            map.put("prop." + item.getKey().toLowerCase(), item.getValue());
        }

        for (Entry<String, Object> item : this.getLib().entrySet()) {
            map.put("lib." + item.getKey().toLowerCase(), item.getValue());
        }

        return map;
    }

}
