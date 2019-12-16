package cn.tsign.common.config;

import java.util.Hashtable;

public class ConfTest {

    protected static Hashtable<ConfigConstant, String> conf = new Hashtable<>();

    static {
        // ##HDFS
        conf.put(ConfigConstant.hdfs_uri, "hdfs://ambari02:8020");
        conf.put(ConfigConstant.hdfs_conf_dir, "/usr/hdp/2.6.1.0-129/hadoop/etc/hadoop");

        // ##Spark
        conf.put(ConfigConstant.app_name, "monitor-collector");
        conf.put(ConfigConstant.master, "yarn");

        // ##kafka
        conf.put(ConfigConstant.zk_quorum, "ambari03:2181,ambari04:2181,ambari05:2181");
        conf.put(ConfigConstant.group, "spark");
        conf.put(ConfigConstant.binlog_topic, "binlog_pool");
        conf.put(ConfigConstant.collect_topic, "bi-test");
        conf.put(ConfigConstant.num_threads, "30");

        // Hbase
        conf.put(ConfigConstant.hbase_zk_port, "2181");
        conf.put(ConfigConstant.hbase_zk_quorum, "ambari03,ambari04,ambari05");
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
        conf.put(ConfigConstant.alarms_conf_path, "/user/spark/alarms.properties");
        conf.put(ConfigConstant.alarms_notification_path, "/user/spark/alarms-notification.properties");
        conf.put(ConfigConstant.druid_agg_data_dir, "/user/spark/druid/druid_data");
        conf.put(ConfigConstant.druid_agg_data_pushed_dir, "/user/spark/druid/druid_pushed");
        conf.put(ConfigConstant.druid_agg_data_push_log_dir, "/user/spark/druid/druid_log");
        conf.put(ConfigConstant.druid_overlord_host, "druid01");
        conf.put(ConfigConstant.druid_overlord_port, "8090");

    }

    public static Hashtable<ConfigConstant, String> getConf() {
        return conf;
    }

}
