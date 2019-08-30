package cn.tsign.spark.broadcast.entity;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.ui.entity.AggMonitorValue;

public class AggMonitor implements Serializable {

    private static final long                          serialVersionUID = 4708952593856630622L;
    private ConcurrentHashMap<String, AggMonitorValue> aggMonitorMap    = new ConcurrentHashMap<>();

    public void put(AggMonitorValue aggMonitorValue) {
        if (aggMonitorValue.getOpType() == null
            || !aggMonitorValue.getOpType().equalsIgnoreCase(CommonConstant.OP_TYPE_AGG)) {
            return;
        }
        agg(aggMonitorValue);

    }

    private void agg(AggMonitorValue monitorValue) {
        String key = monitorValue.getUuid();

        AggMonitorValue oldValue = aggMonitorMap.get(key);
        if (oldValue == null) {
            aggMonitorMap.put(key, monitorValue);
        } else {
            oldValue.setLastOpTime(monitorValue.getLastOpTime());
            oldValue.setOpCount(oldValue.getOpCount().add(monitorValue.getOpCount()));
            aggMonitorMap.put(key, oldValue);
        }
    }

    public ConcurrentHashMap<String, AggMonitorValue> getAggHistoryMap() {
        return aggMonitorMap;
    }

}
