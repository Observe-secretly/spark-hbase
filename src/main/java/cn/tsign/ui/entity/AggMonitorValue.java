package cn.tsign.ui.entity;

import java.io.Serializable;

public class AggMonitorValue extends MonitorValue implements Serializable {

    private static final long serialVersionUID = -633362246067271798L;

    /**
     * 聚合配置监控，由于一个数据源对应多个聚合配置，所以需要一个唯一id定位
     */
    private String            uuid;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

}
