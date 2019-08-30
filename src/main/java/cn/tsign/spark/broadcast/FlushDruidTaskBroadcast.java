package cn.tsign.spark.broadcast;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.google.gson.Gson;

import cn.tsign.common.config.ConfigConstant;
import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.druid.util.DruidTaskInfo;
import cn.tsign.common.druid.util.DruidUtils;
import cn.tsign.common.util.ConfProperties;
import cn.tsign.common.util.HdfsPropFileUtil;
import cn.tsign.common.util.HdfsUtils;
import cn.tsign.common.util.StringUtil;

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

                String todayJsonFile = DruidUtils.getTodayDruidDataFileNameSuffix();

                // 读取配置
                Properties druidTaskInfoConfig = new Properties();
                druidTaskInfoConfig.load(hdfsUtils.readHDFSFileToStream(HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_DRUID_TASK)));

                for (String file : allFiles) {
                    if (file.endsWith(".json")) {
                        // 排除今天的文件
                        if (file.endsWith(todayJsonFile)) continue;
                        // 排除在配置文件中出现的文件，文件中包含已经提交和提交失败的数据文件记录

                        String fileName = DruidUtils.getFileName(file);
                        Object druidTaskInfoObj = druidTaskInfoConfig.get(fileName);

                        DruidTaskInfo druidTaskInfo = (druidTaskInfoObj == null) ? null : new Gson().fromJson(druidTaskInfoObj.toString(),
                                                                                                              DruidTaskInfo.class);
                        if (druidTaskInfo == null) {
                            result.add(file);
                        } else if (!druidTaskInfo.isError()
                                   && (druidTaskInfo.getStatus().equalsIgnoreCase("pending")
                                       || druidTaskInfo.getStatus().equalsIgnoreCase("waiting")
                                       || druidTaskInfo.getStatus().equalsIgnoreCase("running"))) {
                                           // 更新处于Pending、Waiting和Running状态的文件状态
                                           updateDruidTask(druidTaskInfo, hdfsUtils);
                                       }

                    }
                }

                instance = jsc.broadcast(result);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return instance;
    }

    private static void updateDruidTask(DruidTaskInfo druidTaskInfo, HdfsUtils hdfsUtils) {
        new Thread(new Runnable() {

            public void run() {
                System.out.println();
                String status = DruidUtils.getTaskStatus(druidTaskInfo.getTaskId());
                if (StringUtil.isEmpty(status)) {
                    druidTaskInfo.setError(true);
                    druidTaskInfo.setErrorMessage("can not status to task");
                } else {
                    druidTaskInfo.setStatus(status);
                }
                try {
                    HdfsPropFileUtil.updateConf(HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_DRUID_TASK),
                                                DruidUtils.getFileName(druidTaskInfo.getPath()),
                                                druidTaskInfo.toString(), hdfsUtils);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

}
