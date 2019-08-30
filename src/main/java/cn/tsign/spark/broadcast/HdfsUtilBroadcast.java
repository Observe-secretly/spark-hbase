package cn.tsign.spark.broadcast;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import cn.tsign.common.util.HdfsUtils;

public class HdfsUtilBroadcast {

    private static Broadcast<HdfsUtils> instance = null;

    public static synchronized Broadcast<HdfsUtils> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            System.out.println("获取HDFSClient");
            instance = jsc.broadcast(new HdfsUtils());
        }
        return instance;
    }

}
