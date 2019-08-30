package cn.tsign.spark.broadcast;

import org.apache.hadoop.hbase.TableName;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.alibaba.fastjson.JSON;

import cn.tsign.common.hbase.HbaseUtil;

public class HbaseTableNamesBroadcast {

    private static Broadcast<String[]> instance    = null;

    private static Long                refreshTime = null;

    public static synchronized Broadcast<String[]> getInstance(JavaSparkContext jsc, HbaseUtil client) {

        // 定时更新清空配置（更新）
        if (refreshTime != null) {
            long diff = System.currentTimeMillis() - refreshTime;
            if (diff > (60000 * 1)) {
                if (instance != null) {
                    instance.unpersist();
                    instance = null;
                }
            }
        }

        if (instance == null) {
            System.out.print("拉取Hbase表名...");
            TableName[] tableNames = client.getListTableNames();
            String[] result = new String[tableNames.length];
            if (tableNames != null && tableNames.length > 0) {
                for (int i = 0; i < tableNames.length; i++) {
                    result[i] = tableNames[i].getNameAsString();
                }
            }
            instance = jsc.broadcast(result);
            System.out.println(JSON.toJSONString(result));
            refreshTime = System.currentTimeMillis();
        }
        return instance;

    }

}
