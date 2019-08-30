package cn.tsign.common.druid.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import cn.tsign.common.config.ConfigConstant;
import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.network.HttpClientUtil;
import cn.tsign.common.network.HttpResponse;
import cn.tsign.common.util.ConfProperties;
import cn.tsign.common.util.StringUtil;

public class DruidUtils {

    /**
     * 获取当天产生的文件后缀
     * 
     * @param sourceName
     * @return
     */
    public static String getTodayDruidDataFileName(String sourceName) {
        return sourceName + "_" + getTodayDruidDataFileNameSuffix();
    }

    /**
     * 获取当天产生的json文件后缀<br>
     * 可能有多个，因为他们的dataSource可能不同
     * 
     * @return
     */
    public static String getTodayDruidDataFileNameSuffix() {
        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYYMMDD);
        return format.format(new Date()) + ".json";
    }

    /**
     * 根据druid数据文件路径获取sourceName
     * 
     * @param path
     * @return
     */
    public static String getSourceNameForFile(String path) {
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index + 1);
        index = fileName.indexOf("_");
        String sourceName = fileName.substring(0, index);
        return sourceName;
    }

    /**
     * 获取文件名中的时间。这个时间标志着此数据文件的数据是属于那天的
     * 
     * @param path
     * @return
     * @throws ParseException
     */
    public static Date getFileInterval(String path) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYYMMDD);
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index + 1);
        index = fileName.indexOf("_");
        String interval = fileName.substring(index + 1, fileName.length()).replace(".json", "");
        return format.parse(interval);
    }

    /**
     * 根据druid数据文件路径获取文件名称
     * 
     * @param path
     * @return
     */
    public static String getFileName(String path) {
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index + 1);
        return fileName;
    }

    /**
     * 发送数据给druid
     * 
     * @param body
     * @return
     */
    public static String sendDataDruid(String body) {
        String url = "http://" + ConfProperties.getStringValue(ConfigConstant.druid_overlord_host) + ":"
                     + ConfProperties.getIntegerValue(ConfigConstant.druid_overlord_port) + "/druid/indexer/v1/task";
        try {
            HttpResponse response = HttpClientUtil.httpPostRaw(url, body, null, "utf-8");

            if (response != null && !StringUtil.isEmpty(response.getBody())) {
                if (response.getStatusCode() == 200) {
                    // 解析返回值，拿到taskid
                    JSONObject json = JSON.parseObject(response.getBody());
                    String task = json.getString("task");
                    return task;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据taskId查询task状态
     * 
     * @param taskId
     * @return
     */
    public static String getTaskStatus(String taskId) {
        String url = "http://" + ConfProperties.getStringValue(ConfigConstant.druid_overlord_host) + ":"
                     + ConfProperties.getIntegerValue(ConfigConstant.druid_overlord_port) + "/druid/indexer/v1/task/"
                     + taskId + "/status";

        try {
            HttpResponse response = HttpClientUtil.httpGet(url, null, "utf-8");
            if (response != null && !StringUtil.isEmpty(response.getBody())) {
                if (response.getStatusCode() == 200) {
                    // 解析返回值，拿到taskid
                    JSONObject json = JSON.parseObject(response.getBody());
                    JSONObject status = json.getJSONObject("status");
                    if (status != null) {
                        return status.getString("status");
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
