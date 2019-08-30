package cn.tsign.common.druid.datasources.hdfs;

public class MetricsSpec {

    /**
     * 聚合器类型
     */
    private String type;

    /**
     * 输出的时候显示的字段名称，相当于别名
     */
    private String name;

    /**
     * 字段名称
     */
    private String fieldName;

    public MetricsSpec(MetricsSpecTypeEnum type, String name, String fieldName){
        this.type = type.name();
        this.name = name;
        this.fieldName = fieldName;
    }

    public String getType() {
        return type;
    }

    public void setType(MetricsSpecTypeEnum type) {
        this.type = type.name();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

}
