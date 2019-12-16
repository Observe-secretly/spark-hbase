package cn.tsign.spark.broadcast;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import cn.tsign.common.config.ConfigConstant;
import cn.tsign.common.util.ConfProperties;
import cn.tsign.common.util.HdfsUtils;

/**
 * 此广播变量在处理聚合数据的时候触发，列表包含除了今天外的所有没有被处理的json数据文件<br>
 * 
 * @author limin Jul 25, 2019 10:30:33 AM
 */
public class FlushDruidTaskBroadcast {

    private static Broadcast<List<String>> instance    = null;

    private static Long                    refreshTime = null;

    public static synchronized void clean() {
        if (instance != null) {
            instance.unpersist();
            instance = null;
        }
    }

    public static synchronized Broadcast<List<String>> getInstance(JavaSparkContext jsc, HdfsUtils hdfsUtils) {
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
            try {
                refreshTime = System.currentTimeMillis();
                System.out.println("扫描Druid数据文件");
                // 扫描 ConfigConstant.druid_agg_data_dir 目录下的所有数据文件
                List<String> allFiles = hdfsUtils.listAll(ConfProperties.getStringValue(ConfigConstant.druid_agg_data_dir));

                // 把需要上传到Druid的文件梳理出来放入Broadcast中
                List<String> result = new ArrayList<>();

                for (String file : allFiles) {
                    if (file.endsWith(".json")) {
                        result.add(file);
                    }
                }

                instance = jsc.broadcast(result);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return instance;
    }

}
