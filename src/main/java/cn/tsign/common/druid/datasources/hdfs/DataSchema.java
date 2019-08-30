package cn.tsign.common.druid.datasources.hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSchema {

    protected String              dataSource;

    protected Parser              parser;

    protected Map<String, Object> granularitySpec = new HashMap<String, Object>();

    protected List<MetricsSpec>   metricsSpec     = new ArrayList<MetricsSpec>();

    public DataSchema(String dataSource, Parser parser){
        this.dataSource = dataSource;
        this.parser = parser;
    }

    public void setGranularitySpec(String type, SegmentGranularityEnum segmentGranularity, Object queryGranularity,
                                   String... interval) {
        granularitySpec.put("type", type);
        granularitySpec.put("segmentGranularity", segmentGranularity.name());
        if (queryGranularity != null) {
            granularitySpec.put("queryGranularity", queryGranularity);
        }
        granularitySpec.put("intervals", interval);
    }

    public void putMetricsSpec(MetricsSpec... metricsSpec) {
        for (MetricsSpec item : metricsSpec) {
            this.metricsSpec.add(item);
        }
    }

}
