package cn.tsign.spark.broadcast;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import cn.tsign.common.hbase.HbaseUtil;

public class HbaseClientBroadcast {

    private static Broadcast<HbaseUtil> instance = null;

    public static synchronized Broadcast<HbaseUtil> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            HbaseUtil client = new HbaseUtil();
            instance = jsc.broadcast(client);
        }
        return instance;

    }
}
