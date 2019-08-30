package cn.tsign.spark.accumulator;

import java.util.Map.Entry;

import org.apache.spark.util.AccumulatorV2;

import cn.tsign.spark.broadcast.entity.Monitor;
import cn.tsign.ui.entity.MonitorValue;

public class MonitorAccumulator extends AccumulatorV2<MonitorValue, Monitor> {

    private static final long serialVersionUID = -2977230952943704207L;

    private Monitor           value            = new Monitor();

    @Override
    public void add(MonitorValue monitorValue) {
        value.put(monitorValue);
    }

    @Override
    public AccumulatorV2<MonitorValue, Monitor> copy() {
        MonitorAccumulator ma = new MonitorAccumulator();
        ma.value = this.value();
        return ma;
    }

    @Override
    public boolean isZero() {
        if (value.getDiscardHistoryMap().size() == 0 && value.getSaveHistoryMap().size() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public void merge(AccumulatorV2<MonitorValue, Monitor> arg0) {
        Monitor mergeValue = arg0.value();
        if (mergeValue.getDiscardHistoryMap() != null && mergeValue.getDiscardHistoryMap().size() > 0) {
            for (Entry<String, MonitorValue> item : mergeValue.getDiscardHistoryMap().entrySet()) {
                this.value.put(item.getValue());
            }
        }
        if (mergeValue.getSaveHistoryMap() != null && mergeValue.getSaveHistoryMap().size() > 0) {
            for (Entry<String, MonitorValue> item : mergeValue.getSaveHistoryMap().entrySet()) {
                this.value.put(item.getValue());
            }
        }
    }

    @Override
    public void reset() {
        value = new Monitor();

    }

    @Override
    public Monitor value() {
        return value;
    }

}
