package cn.tsign.common.druid.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import cn.tsign.common.config.ConfigConstant;
import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.constant.FunctionNameConstant;
import cn.tsign.common.druid.datasources.hdfs.SegmentGranularityEnum;
import cn.tsign.common.network.HttpClientUtil;
import cn.tsign.common.network.HttpResponse;
import cn.tsign.common.util.ConfProperties;
import cn.tsign.common.util.StringUtil;
import cn.tsign.entity.AggregationFieldEntity;
import cn.tsign.entity.AggregationOperatorEnum;

public class DruidUtils {

    /**
     * 获取当天产生的文件后缀
     * 
     * @param sourceName
     * @return
     */
    public static String getDruidDataFileName(String sourceName, SegmentGranularityEnum segmentGranularityEnum) {
        return sourceName + "_" + getTodayDruidDataFileNameSuffix(segmentGranularityEnum);
    }

    /**
     * 获取当天产生的json文件后缀<br>
     * 可能有多个，因为他们的dataSource可能不同
     * 
     * @return
     */
    public static String getTodayDruidDataFileNameSuffix(SegmentGranularityEnum segmentGranularityEnum) {

        return getDataformat(segmentGranularityEnum).format(new Date()) + ".json";
    }

    public static SimpleDateFormat getDataformat(SegmentGranularityEnum segmentGranularityEnum) {
        SimpleDateFormat format = null;

        switch (segmentGranularityEnum) {
            case year:
                format = new SimpleDateFormat(CommonConstant.YYYY);
                break;
            case month:
                format = new SimpleDateFormat(CommonConstant.YYYYMM);
                break;
            case day:
                format = new SimpleDateFormat(CommonConstant.YYYYMMDD);
                break;
            case hour:
                format = new SimpleDateFormat(CommonConstant.YYYYMMDDHH);
                break;
            case minute:
                format = new SimpleDateFormat(CommonConstant.YYYYMMDDHHMM);
                break;
            default:
                format = new SimpleDateFormat(CommonConstant.YYYYMMDD);
                break;

        }
        return format;
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
        index = fileName.lastIndexOf("_");
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
    public static Date getFileInterval(String path,
                                       SegmentGranularityEnum segmentGranularityEnum) throws ParseException {
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index + 1);
        index = fileName.lastIndexOf("_");
        String interval = fileName.substring(index + 1, fileName.length()).replace(".json", "");
        return getDataformat(segmentGranularityEnum).parse(interval);
    }

    /**
     * 根据druid数据文件路径获取文件名称
     * 
     * @param path
     * @return
     */
    public static String getDataFileName(String path) {
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index + 1).replace(".json", "");
        return fileName;
    }

    public static String getLogFileName(String path) {
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index + 1).replace(".log", "").replace(".process", "").replace(".err", "");
        return fileName;
    }

    public static String getDataFileFullName(String path) {
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
            HttpResponse response = HttpClientUtil.httpPostRaw(url, body, null, "utf-8", 3000, 1000, 3000);

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

    /**
     * 把 sum(field)、min(field)等聚合函数解释成AggregationFieldEntity对象
     * 
     * @return
     */
    public static AggregationFieldEntity analysisColumnDefinition(String columnDefinition) {
        FunctionNameConstant.FunContent funContent = null;
        AggregationOperatorEnum operator = null;

        if (columnDefinition.startsWith(FunctionNameConstant.AGG_MAX)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_MAX, columnDefinition);
            operator = AggregationOperatorEnum.MAX;
        } else if (columnDefinition.startsWith(FunctionNameConstant.AGG_MIN)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_MIN, columnDefinition);
            operator = AggregationOperatorEnum.MIN;
        } else if (columnDefinition.startsWith(FunctionNameConstant.AGG_SUM)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_SUM, columnDefinition);
            operator = AggregationOperatorEnum.SUM;
        } else if (columnDefinition.startsWith(FunctionNameConstant.AGG_COUNT)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_COUNT, columnDefinition);
            operator = AggregationOperatorEnum.COUNT;
        } else if (columnDefinition.startsWith(FunctionNameConstant.AGG_AVG)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_AVG, columnDefinition);
            operator = AggregationOperatorEnum.AVG;
        } else {
            // 错误不支持的函数
            return null;
        }

        return new AggregationFieldEntity(funContent.getAlias(), operator, funContent.getContent());

    }

}
