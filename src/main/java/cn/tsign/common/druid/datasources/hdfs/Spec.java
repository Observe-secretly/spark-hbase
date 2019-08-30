package cn.tsign.common.druid.datasources.hdfs;

import java.util.HashMap;
import java.util.Map;

public class Spec {

    protected Map<String, Object> tuningConfig = new HashMap<String, Object>();

    protected Map<String, Object> ioConfig     = new HashMap<String, Object>();

    protected DataSchema          dataSchema;

    public Spec(DataSchema dataSchema){
        this.dataSchema = dataSchema;
    }

    public void setTuningConfig(String partitionsType, long targetPartitionSize) {

        Map<String, Object> partitionsSpec = new HashMap<String, Object>();
        partitionsSpec.put("type", partitionsType);
        partitionsSpec.put("targetPartitionSize", targetPartitionSize);

        tuningConfig.put("type", "hadoop");
        tuningConfig.put("partitionsSpec", partitionsSpec);
    }

    public void setIoConfig(String inputType, String paths) {
        Map<String, Object> inputSpec = new HashMap<String, Object>();
        inputSpec.put("type", inputType);
        inputSpec.put("paths", paths);

        ioConfig.put("type", "hadoop");
        ioConfig.put("inputSpec", inputSpec);
    }

}
