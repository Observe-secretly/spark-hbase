package cn.tsign.common.druid.datasources.hdfs;

import java.util.HashMap;
import java.util.Map;

public class Parser {

    protected String              type      = "hadoopyString";

    protected Map<String, Object> parseSpec = new HashMap<>();

    /**
     * 设置上传时的数据格式
     * 
     * @param format
     */
    public void setParseSpecFormat(String format) {
        this.parseSpec.put("format", format);
    }

    /**
     * 设置时间戳列
     * 
     * @param column
     * @param format 默认值：auto
     */
    public void setTimestampSpec(String column, String format) {
        if (format == null || format.trim().equalsIgnoreCase("")) {
            format = "auto";
        }
        Map<String, Object> timestampSpec = new HashMap<>();
        timestampSpec.put("column", column);
        timestampSpec.put("format", format);

        parseSpec.put("timestampSpec", timestampSpec);

    }

    /**
     * 设置属性列等属性
     * 
     * @param dimensionExclusions
     * @param spatialDimensions
     * @param dimensions
     */
    public void setDimensionsSpec(String[] dimensionExclusions, String[] spatialDimensions, String... dimensions) {
        Map<String, Object> dimensionsSpec = new HashMap<String, Object>();
        if (dimensionExclusions != null) {
            dimensionsSpec.put("dimensionExclusions", dimensionExclusions);
        }
        if (spatialDimensions != null) {
            dimensionsSpec.put("spatialDimensions", spatialDimensions);
        }

        dimensionsSpec.put("dimensions", dimensions);

        parseSpec.put("dimensionsSpec", dimensionsSpec);
    }

}
