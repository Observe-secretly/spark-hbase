package cn.tsign.common.druid.datasources.hdfs;

public class Config {

    protected String type = "index_hadoop";

    protected Spec   spec;

    public Spec getSpec() {
        return spec;
    }

    public void setSpec(Spec spec) {
        this.spec = spec;
    }

}
