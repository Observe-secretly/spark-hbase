package cn.tsign.common.config;

public enum ConfigConstant {
                            // Other
                            collector_poll_time("collector.poll.time"), //
                            log_level("log.level"), //
                            track_tablename_conf_path("track.tablename.conf.path"), //
                            binlog_tablename_conf_path("binlog.tablename.conf.path"), //
                            rowkey_conf_path("rowkey.conf.path"), //
                            agg_conf_path("agg.conf.path"), //
                            druid_task_conf_path("druid.task.conf.path"), //
                            druid_agg_data_dir("druid.agg.data.path"), //
                            druid_overlord_host("druid.overlord.host"), //
                            druid_overlord_port("druid.overlord.port"), //

                            // Hbase
                            hbase_zk_port("hbase.zk.port"), //
                            hbase_zk_quorum("hbase.zk.quorum"), //
                            hbase_master("hbase.master"), //
                            hbase_znode_parent("hbase.znode.parent"), //
                            hbase_is_storage("hbase.is.storage"),
                            // Kafka
                            zk_quorum("zk.quorum"), //
                            group("group"), //
                            binlog_topic("binlog.topic"), //
                            collect_topic("collect.topic"), //
                            num_threads("numThreads"),
                            // Spark
                            master("master"), //
                            app_name("app.name"),

                            // HDFS
                            hdfs_uri("hdfs.uri"), //
                            hdfs_conf_dir("hdfs.conf.dir");

    private String name;

    private ConfigConstant(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
