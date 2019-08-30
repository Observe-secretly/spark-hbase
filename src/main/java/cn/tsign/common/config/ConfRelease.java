package cn.tsign.common.config;

import java.util.Hashtable;

public class ConfRelease {

    protected static Hashtable<ConfigConstant, String> conf = new Hashtable<>();

    static {
        // ##HDFS
        conf.put(ConfigConstant.hdfs_uri, "hdfs://ha01:8020");
        conf.put(ConfigConstant.hdfs_conf_dir, "/usr/hdp/2.6.1.0-129/hadoop/etc/hadoop");

        // ##Spark
        conf.put(ConfigConstant.app_name, "monitor-collector");
        conf.put(ConfigConstant.master, "yarn-cluster");

        // ##kafka
        conf.put(ConfigConstant.zk_quorum, "h3:2181,h4:2181,h8:2181");
        conf.put(ConfigConstant.group, "spark");
        conf.put(ConfigConstant.binlog_topic,
                 "binlog-application binlog-realname binlog-billing binlog-common binlog-evidence binlog-sign");
        conf.put(ConfigConstant.collect_topic, "datacollect");
        conf.put(ConfigConstant.num_threads, "30");

        // Hbase
        conf.put(ConfigConstant.hbase_zk_port, "2181");
        conf.put(ConfigConstant.hbase_zk_quorum, "h3,h4,h8");
        conf.put(ConfigConstant.hbase_master, "");
        conf.put(ConfigConstant.hbase_znode_parent, "/hbase-unsecure");
        conf.put(ConfigConstant.hbase_is_storage, "true");

        // ##Other
        conf.put(ConfigConstant.collector_poll_time, "2500");// Collector settings.Unit: millisecond
        conf.put(ConfigConstant.log_level, "WARN");
        conf.put(ConfigConstant.track_tablename_conf_path, "/user/spark/track-hbase-tablename-conf.properties");
        conf.put(ConfigConstant.binlog_tablename_conf_path, "/user/spark/binlog-hbase-tablename-conf.properties");
        conf.put(ConfigConstant.rowkey_conf_path, "/user/spark/hbase-rowkey-conf.properties");
        conf.put(ConfigConstant.agg_conf_path, "/user/spark/aggregation-conf.properties");
        conf.put(ConfigConstant.druid_task_conf_path, "/user/spark/druid-task-conf-path.properties");
        conf.put(ConfigConstant.druid_agg_data_dir, "/user/spark/druid");
        conf.put(ConfigConstant.druid_overlord_host, "druid01");
        conf.put(ConfigConstant.druid_overlord_port, "8090");
    }

    public static Hashtable<ConfigConstant, String> getConf() {
        return conf;
    }

}
