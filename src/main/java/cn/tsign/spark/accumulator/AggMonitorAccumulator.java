package cn.tsign.spark.accumulator;

import java.util.Map.Entry;

import org.apache.spark.util.AccumulatorV2;

import cn.tsign.spark.broadcast.entity.AggMonitor;
import cn.tsign.ui.entity.AggMonitorValue;

public class AggMonitorAccumulator extends AccumulatorV2<AggMonitorValue, AggMonitor> {

    private static final long serialVersionUID = -2977230952943704207L;

    private AggMonitor        value            = new AggMonitor();

    @Override
    public void add(AggMonitorValue monitorValue) {
        value.put(monitorValue);
    }

    @Override
    public AccumulatorV2<AggMonitorValue, AggMonitor> copy() {
        AggMonitorAccumulator ma = new AggMonitorAccumulator();
        ma.value = this.value();
        return ma;
    }

    @Override
    public boolean isZero() {
        if (value.getAggHistoryMap().size() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public void merge(AccumulatorV2<AggMonitorValue, AggMonitor> arg0) {
        AggMonitor mergeValue = arg0.value();
        if (mergeValue.getAggHistoryMap() != null && mergeValue.getAggHistoryMap().size() > 0) {
            for (Entry<String, AggMonitorValue> item : mergeValue.getAggHistoryMap().entrySet()) {
                this.value.put(item.getValue());
            }
        }
    }

    @Override
    public void reset() {
        value = new AggMonitor();

    }

    @Override
    public AggMonitor value() {
        return value;
    }

}
