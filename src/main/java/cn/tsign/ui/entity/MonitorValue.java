package cn.tsign.ui.entity;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Date;

import cn.tsign.common.constant.SinkEnum;

public class MonitorValue implements Serializable {

    private static final long serialVersionUID = -2181533275600951905L;

    /**
     * 保存的hbaseTable表名或者丢弃的数据理论（可以在confTab上看到理论值的映射逻辑）上的表名
     */
    private String            hbaseTable;

    /**
     * 数据存储到hbase了还是druid
     */
    private SinkEnum          sink;

    /**
     * 当sink是hbase时：真实保存到Hbase的目标表。（真实存在的表）<br>
     * 当sink是druid时：表示数据源名称
     */
    private String            sourceName;

    /**
     * 最后保存或丢弃时间
     */
    private Date              lastOpTime;

    /**
     * 操作次数用来描述SparkStreaming从开启到目前为止hbaseTable保存了多少数据量的数据，或者丢弃了多少数据
     */
    private BigInteger        opCount          = BigInteger.ZERO;

    /**
     * 操作类型。是save还是discard
     */
    private String            opType;

    /**
     * track or binlog
     */
    private String            type;

    public String getHbaseTable() {
        return hbaseTable;
    }

    public void setHbaseTable(String hbaseTable) {
        this.hbaseTable = hbaseTable;
    }

    public Date getLastOpTime() {
        return lastOpTime;
    }

    public void setLastOpTime(Date lastOpTime) {
        this.lastOpTime = lastOpTime;
    }

    public BigInteger getOpCount() {
        return opCount;
    }

    public void setOpCount(BigInteger opCount) {
        this.opCount = opCount;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
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

}
