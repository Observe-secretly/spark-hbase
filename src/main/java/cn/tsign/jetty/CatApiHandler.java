package cn.tsign.jetty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import cn.tsign.spark.broadcast.entity.AggMonitor;
import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.spark.broadcast.entity.CoreConfig.AggConfig;
import cn.tsign.spark.broadcast.entity.Monitor;
import cn.tsign.ui.entity.AggMonitorValue;
import cn.tsign.ui.entity.MonitorValue;

public class CatApiHandler {

    public static String cat(HttpServletRequest request, HttpServletResponse response) {
        StringBuilder result = new StringBuilder();
        for (CatApiPathEnum item : CatApiPathEnum.values()) {
            result.append(item.path);
            result.append("\n");
        }
        return result.toString();
    }

    public static String catFuse(CoreConfig coreConfig, Monitor monitor, AggMonitor aggMonitor,
                                 HttpServletRequest request, HttpServletResponse response) {
        Map<String, MonitorValue> trackFuse = new HashMap<>();
        for (Entry<String, String> item : coreConfig.getTrackTableNameSettings().entrySet()) {
            trackFuse.put(item.getKey(), monitor.getSaveHistoryMap().get(item.getKey()));
        }

        Map<String, MonitorValue> binlogFuse = new HashMap<>();
        for (Entry<String, String> item : coreConfig.getBinlogTableNameSettings().entrySet()) {
            binlogFuse.put(item.getKey(), monitor.getSaveHistoryMap().get(item.getKey()));
        }

        List<AggMonitorValue> druidFuse = new ArrayList<>();
        for (Entry<String, List<AggConfig>> entry : coreConfig.getAggSettings().entrySet()) {
            for (AggConfig item : entry.getValue()) {
                druidFuse.add(aggMonitor.getAggHistoryMap().get(item.getUuid()));
            }
        }

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("track", trackFuse);
        resultMap.put("binlog", binlogFuse);
        resultMap.put("druid", druidFuse);
        return new GsonBuilder().serializeNulls().create().toJson(resultMap);

    }

    public static String catConfig(CoreConfig coreConfig, HttpServletRequest request, HttpServletResponse response) {
        return new Gson().toJson(coreConfig);

    }

    public static String catConfigTrack(CoreConfig coreConfig, HttpServletRequest request,
                                        HttpServletResponse response) {
        return new Gson().toJson(coreConfig.getTrackTableNameSettings());

    }

    public static String catConfigBinlog(CoreConfig coreConfig, HttpServletRequest request,
                                         HttpServletResponse response) {
        return new Gson().toJson(coreConfig.getBinlogTableNameSettings());

    }

    public static String catConfigRowkey(CoreConfig coreConfig, HttpServletRequest request,
                                         HttpServletResponse response) {
        return new Gson().toJson(coreConfig.getRowkeySettings());

    }

    public static String catConfigAgg(CoreConfig coreConfig, HttpServletRequest request, HttpServletResponse response) {
        return new Gson().toJson(coreConfig.getAggSettings());

    }

    public static String catMonitor(Monitor monitor, HttpServletRequest request, HttpServletResponse response) {
        return new Gson().toJson(monitor);

    }

    public static String catMonitorSave(Monitor monitor, HttpServletRequest request, HttpServletResponse response) {
        return new Gson().toJson(monitor.getSaveHistoryMap());

    }

    public static String catMonitorDiscard(Monitor monitor, HttpServletRequest request, HttpServletResponse response) {
        return new Gson().toJson(monitor.getDiscardHistoryMap());

    }

    public static String catMonitorAggregation(AggMonitor monitor, HttpServletRequest request,
                                               HttpServletResponse response) {
        return new Gson().toJson(monitor.getAggHistoryMap());

    }

}
