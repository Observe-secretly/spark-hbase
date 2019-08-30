package cn.tsign.jetty;

public enum CatApiPathEnum {
                            _cat_monitor("/_cat/monitor", "save history 、 discard history 、aggregation history "), //
                            _cat_monitor_save("/_cat/monitor/save", "save history "), //
                            _cat_monitor_discard("/_cat/monitor/discard", "discard history "), //
                            _cat_monitor_agg("/_cat/monitor/agg", "aggregation history "), //
                            _cat_config("/_cat/config", "Track Config,Binlog Config,Rowkey Config,Aggregation Config"), //
                            _cat_config_track("/_cat/config/track", "Track Config"), //
                            _cat_config_binlog("/_cat/config/binlog", "Binlog Config"), //
                            _cat_config_rowkey("/_cat/config/rowkey", "Binlog Config"), //
                            _cat_config_agg("/_cat/config/agg", "Aggregation Config"), //
                            _cat_config_druid_push("/_cat/druid/push", "druid push history"), //
                            _cat_fuse("/_cat/fuse", "config join monitor"), //
                            _cat("/_cat", "api desc list");//

    public String path;
    public String desc;

    CatApiPathEnum(String path, String desc){
        this.path = path;
        this.desc = desc;
    }

    public static CatApiPathEnum get(String path) {
        for (CatApiPathEnum item : CatApiPathEnum.values()) {
            if (item.path.equalsIgnoreCase(path)) {
                return item;
            }
        }
        return null;
    }

}
