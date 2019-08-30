package cn.tsign.spark.broadcast.entity;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.ui.entity.MonitorValue;

public class Monitor implements Serializable {

    private static final long                       serialVersionUID  = 3197850958779667801L;

    private ConcurrentHashMap<String, MonitorValue> saveHistoryMap    = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, MonitorValue> discardHistoryMap = new ConcurrentHashMap<>();

    public void put(MonitorValue monitorValue) {
        if (monitorValue.getOpType() == null) {
            return;
        }
        switch (monitorValue.getOpType()) {
            case CommonConstant.OP_TYPE_SAVE:
                save(monitorValue);
                break;
            case CommonConstant.OP_TYPE_DISCARD:
                discard(monitorValue);
                break;
            default:
                return;
        }

    }

    private void save(MonitorValue monitorValue) {
        String key = monitorValue.getHbaseTable();
        if (key == null) {
            return;
        }

        MonitorValue oldValue = saveHistoryMap.get(key);
        if (oldValue == null) {
            saveHistoryMap.put(key, monitorValue);
        } else {
            oldValue.setLastOpTime(monitorValue.getLastOpTime());
            oldValue.setOpCount(oldValue.getOpCount().add(monitorValue.getOpCount()));
            saveHistoryMap.put(key, oldValue);
        }
    }

    private void discard(MonitorValue monitorValue) {
        String key = monitorValue.getHbaseTable();
        if (key == null) {
            return;
        }

        MonitorValue oldValue = discardHistoryMap.get(key);
        if (oldValue == null) {
            discardHistoryMap.put(key, monitorValue);
        } else {
            oldValue.setLastOpTime(monitorValue.getLastOpTime());
            oldValue.setOpCount(oldValue.getOpCount().add(monitorValue.getOpCount()));
            discardHistoryMap.put(key, oldValue);
        }
    }

    public ConcurrentHashMap<String, MonitorValue> getSaveHistoryMap() {
        return saveHistoryMap;
    }

    public ConcurrentHashMap<String, MonitorValue> getDiscardHistoryMap() {
        return discardHistoryMap;
    }

}
