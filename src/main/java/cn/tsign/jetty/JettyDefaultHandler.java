package cn.tsign.jetty;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.Gson;

import cn.tsign.common.config.ConfigConstant;
import cn.tsign.common.config.Env;
import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.constant.FunctionNameConstant;
import cn.tsign.common.constant.SinkEnum;
import cn.tsign.common.druid.datasources.DruidConfig;
import cn.tsign.common.druid.datasources.DruidConfig.HdfsParam;
import cn.tsign.common.druid.datasources.hdfs.MetricsSpec;
import cn.tsign.common.druid.datasources.hdfs.MetricsSpecTypeEnum;
import cn.tsign.common.druid.datasources.hdfs.SegmentGranularityEnum;
import cn.tsign.common.druid.util.DruidTaskInfo;
import cn.tsign.common.druid.util.DruidUtils;
import cn.tsign.common.hbase.HbaseUtil;
import cn.tsign.common.network.HttpClientUtil;
import cn.tsign.common.util.ConfProperties;
import cn.tsign.common.util.HdfsPropFileUtil;
import cn.tsign.common.util.HdfsUtils;
import cn.tsign.common.util.StringUtil;
import cn.tsign.entity.AggregationFieldEntity;
import cn.tsign.spark.accumulator.AggMonitorAccumulator;
import cn.tsign.spark.accumulator.MonitorAccumulator;
import cn.tsign.spark.broadcast.ConfBroadcast;
import cn.tsign.spark.broadcast.FlushDruidTaskBroadcast;
import cn.tsign.spark.broadcast.HbaseClientBroadcast;
import cn.tsign.spark.broadcast.HdfsUtilBroadcast;
import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.spark.broadcast.entity.CoreConfig.AggConfig;
import cn.tsign.ui.BodyTemplate;
import cn.tsign.ui.HeadTemplate;
import cn.tsign.ui.entity.MonitorUIConfig;
import cn.tsign.ui.entity.MonitorValue;
import cn.tsign.ui.js.AlarmsJs;
import cn.tsign.ui.js.AlertJs;
import cn.tsign.ui.js.BlockJs;
import cn.tsign.ui.tab.AlarmsTabUI;
import cn.tsign.ui.tab.ConfTabUI;
import cn.tsign.ui.tab.DruidPushTabUI;
import cn.tsign.ui.tab.MonitorNetworkTabUI;
import cn.tsign.ui.tab.MonitorTabUI;
import cn.tsign.ui.tab.OperatorTabUI;
import cn.tsign.ui.tab.SettingsTabUI;

public class JettyDefaultHandler extends AbstractHandler {

    JavaSparkContext         sc;
    JavaStreamingContext     jssc;
    Server                   server;
    MonitorAccumulator       monitorAccumulator;
    AggMonitorAccumulator    aggMonitorAccumulator;

    ScheduledExecutorService pool = null;

    public JettyDefaultHandler(){
    }

    public JettyDefaultHandler(JavaSparkContext sc, JavaStreamingContext jssc, MonitorAccumulator monitorAccumulator,
                               AggMonitorAccumulator aggMonitorAccumulator, Server server){
        this.sc = sc;
        this.jssc = jssc;
        this.monitorAccumulator = monitorAccumulator;
        this.aggMonitorAccumulator = aggMonitorAccumulator;
        this.server = server;
        this.pool = Executors.newScheduledThreadPool(1);
        startTimer();
    }

    public void startTimer() {
        pool.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    // 扫描德鲁伊数据文件，把需要推送的文件，推送到Druid
                    CoreConfig coreConfig = ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue();
                    pushDataToDriud(sc, coreConfig.getAggSettings());

                    // 扫描德鲁伊日志文件。跟踪process状态的推送任务
                    trackDruidTask(sc);

                    // 清理超过24小时的Druid推送成功日志和超过7*24小时的失败日志
                    cleanDruidTaskLog(sc);

                    Map<String, String> notifyConf = coreConfig.getAlarmNotificationConfig();

                    // 扫描并发送告警
                    scanAlarms(monitorAccumulator.value().getSaveHistoryMap(), coreConfig.getTrackTableNameSettings(),
                               coreConfig.getBinlogTableNameSettings(), coreConfig.getAlarmConfig(),
                               notifyConf.get(CommonConstant.NOTIFY_CONF_POST_URL),
                               notifyConf.get(CommonConstant.NOTIFY_CONF_BODY));
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        }, 0, 1000 * 60, TimeUnit.MILLISECONDS);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException {
        // 判断是不是catapi
        int catIndex = target.lastIndexOf("/_cat");
        if (catIndex != -1) {
            String catApiPath = target.substring(catIndex);
            catApiRouter(catApiPath, request, response);
        } else if (target.trim().equalsIgnoreCase("") || target.endsWith("/") || target.endsWith("/conf")) {
            confView(baseRequest, request, response);
        } else if (target.endsWith("monitor")) {
            monitorView(baseRequest, request, response);
        } else if (target.endsWith("monitorNetwork")) {
            monitorNetworkView(baseRequest, request, response);
        } else if (target.endsWith("druidPush")) {
            druidPushView(baseRequest, request, response);
        } else if (target.endsWith("operator")) {
            operatorView(baseRequest, request, response);
        } else if (target.endsWith("closeAction")) {
            closeAction(baseRequest, request, response);
        } else if (target.endsWith("settings")) {
            settingView(baseRequest, request, response);
        } else if (target.endsWith("addMappingConfAction")) {
            addMappingConfAction(baseRequest, request, response);
        } else if (target.endsWith("addRowkeyConfAction")) {
            addRowkeyConfAction(baseRequest, request, response);
        } else if (target.endsWith("removeConfAction")) {
            removeConfAction(baseRequest, request, response);
        } else if (target.endsWith("refreshConfAction")) {
            refreshConfAction(baseRequest, request, response);
        } else if (target.endsWith("addAggConfAction")) {
            addAggConfAction(baseRequest, request, response);
        } else if (target.endsWith("removeAggConfAction")) {
            removeAggConfAction(baseRequest, request, response);
        } else if (target.endsWith("alarms")) {
            alarmsView(baseRequest, request, response);
        } else if (target.endsWith("updateAlarmsTimeout")) {
            updateAlarmsTimeoutAction(baseRequest, request, response);
        } else if (target.endsWith("updateAlarmsNotifyConf")) {
            updateAlarmsNotifyConf(baseRequest, request, response);
        }

    }

    /**
     * 如果是api接口，则走这里路由。这里是模仿elasticsearch的api模式
     * 
     * @param catApiPath
     * @param request
     * @param response
     * @throws IOException
     */
    private void catApiRouter(String catApiPath, HttpServletRequest request,
                              HttpServletResponse response) throws IOException {
        String body = null;
        CatApiPathEnum reqPath = CatApiPathEnum.get(catApiPath);
        if (reqPath == null) {
            response.sendRedirect("/_cat");
            return;
        }

        switch (reqPath) {
            case _cat:
                body = CatApiHandler.cat(request, response);
                break;
            case _cat_monitor:
                body = CatApiHandler.catMonitor(monitorAccumulator.value(), request, response);
                break;
            case _cat_monitor_save:
                body = CatApiHandler.catMonitorSave(monitorAccumulator.value(), request, response);
                break;
            case _cat_monitor_discard:
                body = CatApiHandler.catMonitorDiscard(monitorAccumulator.value(), request, response);
                break;
            case _cat_monitor_agg:
                body = CatApiHandler.catMonitorAggregation(aggMonitorAccumulator.value(), request, response);
                break;
            case _cat_config:
                body = CatApiHandler.catConfig(ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                               request, response);
                break;
            case _cat_config_track:
                body = CatApiHandler.catConfigTrack(ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                                    request, response);
                break;
            case _cat_config_binlog:
                body = CatApiHandler.catConfigBinlog(ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                                     request, response);
                break;
            case _cat_config_rowkey:
                body = CatApiHandler.catConfigRowkey(ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                                     request, response);
                break;
            case _cat_config_agg:
                body = CatApiHandler.catConfigAgg(ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                                  request, response);
                break;
            case _cat_fuse:
                body = CatApiHandler.catFuse(ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                             monitorAccumulator.value(), aggMonitorAccumulator.value(), request,
                                             response);
                break;

            default:
                return;
        }

        print(body, response);
    }

    private void confView(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        HeadTemplate head = new HeadTemplate(sc, "SparkHbaseMonitor Config");
        head.appendElement(new AlertJs().toString());
        head.appendElement(new BlockJs("Config").toString());

        BodyTemplate body = new BodyTemplate(sc, BodyTemplate.CONF);
        body.appendElement(new ConfTabUI(sc, ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                         monitorAccumulator.value().getSaveHistoryMap(),
                                         aggMonitorAccumulator.value().getAggHistoryMap()).toTemplate());

        StringBuilder html = new StringBuilder();
        html.append("<html>");
        html.append(head.toTemplate());
        html.append(body.toTemplate());
        html.append("</html>");
        print(html.toString(), response);

    }

    private void monitorView(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        HeadTemplate head = new HeadTemplate(sc, "SparkHbaseMonitorp Monitor");
        head.appendElement(new BlockJs("Monitor").toString());

        BodyTemplate body = new BodyTemplate(sc, BodyTemplate.MONITOR);
        body.appendElement(new MonitorTabUI(sc, monitorAccumulator.value().getSaveHistoryMap(),
                                            aggMonitorAccumulator.value().getAggHistoryMap(),
                                            monitorAccumulator.value().getDiscardHistoryMap()).toTemplate());

        StringBuilder html = new StringBuilder();
        html.append("<html>");
        html.append(head.toTemplate());
        html.append(body.toTemplate());
        html.append("</html>");
        print(html.toString(), response);

    }

    private void monitorNetworkView(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        HeadTemplate head = new HeadTemplate(sc, "SparkHbaseMonitorp MonitorNetwork");
        head.appendElement(new BlockJs("Monitor").toString());

        BodyTemplate body = new BodyTemplate(sc, BodyTemplate.MonitorNetwork);
        body.appendElement(new MonitorNetworkTabUI(sc, ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                                   monitorAccumulator.value().getSaveHistoryMap()).toTemplate());

        StringBuilder html = new StringBuilder();
        html.append("<html>");
        html.append(head.toTemplate());
        html.append(body.toTemplate());
        html.append("</html>");
        print(html.toString(), response);

    }

    private void druidPushView(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        HeadTemplate head = new HeadTemplate(sc, "SparkHbaseMonitorp MonitorNetwork");
        head.appendElement(new BlockJs("Druid Push").toString());

        BodyTemplate body = new BodyTemplate(sc, BodyTemplate.DruidPush);
        body.appendElement(new DruidPushTabUI(sc,
                                              HdfsUtilBroadcast.getInstance(jssc.sparkContext()).getValue()).toTemplate());

        StringBuilder html = new StringBuilder();
        html.append("<html>");
        html.append(head.toTemplate());
        html.append(body.toTemplate());
        html.append("</html>");
        print(html.toString(), response);

    }

    /**
     * 优雅关闭
     * 
     * @param baseRequest
     * @param request
     * @param response
     */
    private void operatorView(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        HeadTemplate head = new HeadTemplate(sc, "SparkHbaseMonitor Operator");
        head.appendElement(new BlockJs("Operator").toString());

        BodyTemplate body = new BodyTemplate(sc, BodyTemplate.Operator);
        body.appendElement(new OperatorTabUI(sc).toTemplate());

        StringBuilder html = new StringBuilder();
        html.append("<html>");
        html.append(head.toTemplate());
        html.append(body.toTemplate());
        html.append("</html>");
        print(html.toString(), response);
    }

    private void settingView(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        Broadcast<HbaseUtil> hbaseUtilBroadcast = HbaseClientBroadcast.getInstance(jssc.sparkContext());

        BodyTemplate body = new BodyTemplate(sc, BodyTemplate.Settings);
        body.appendElement(new SettingsTabUI(sc, ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                             hbaseUtilBroadcast.getValue().getListTableNames()).toTemplate());

        HeadTemplate head = new HeadTemplate(sc, "SparkHbaseMonitor Settings");
        head.appendElement(new AlertJs().toString());
        head.appendElement(new BlockJs("Settings").toString());

        StringBuilder html = new StringBuilder();
        html.append("<html>");
        html.append(head.toTemplate());
        html.append(body.toTemplate());
        html.append("</html>");
        print(html.toString(), response);
    }

    private void alarmsView(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        BodyTemplate body = new BodyTemplate(sc, BodyTemplate.ALARMS);
        body.appendElement(new AlarmsTabUI(sc, ConfBroadcast.getInstance(jssc.sparkContext(), false).getValue(),
                                           monitorAccumulator.value().getSaveHistoryMap()).toTemplate());

        HeadTemplate head = new HeadTemplate(sc, "SparkHbaseMonitor Settings");
        head.appendElement(new AlertJs().toString());

        head.appendElement(new AlarmsJs().toString());
        head.appendElement(new BlockJs("Alarms").toString());

        StringBuilder html = new StringBuilder();
        html.append("<html>");
        html.append(head.toTemplate());
        html.append(body.toTemplate());
        html.append("</html>");
        print(html.toString(), response);
    }

    /**
     * 执行关闭操作
     * 
     * @param baseRequest
     * @param request
     * @param response
     */
    private void closeAction(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        jssc.stop(true, true);// 优雅的关闭
        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        System.out.println("SparkStreaming关闭成功.....");
        print("关闭成功", response);
        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 添加配置
     * 
     * @param baseRequest
     * @param request
     * @param response
     */
    private synchronized void addMappingConfAction(Request baseRequest, HttpServletRequest request,
                                                   HttpServletResponse response) {
        int errorCode = ErrorCode.SUCCESS;
        String uniqueId = request.getParameter("uniqueId").trim();
        String type = request.getParameter("type").trim();
        String tableName = request.getParameter("tableName").trim();
        if (StringUtil.isEmpty(uniqueId) || StringUtil.isEmpty(type) || StringUtil.isEmpty(tableName)) {
            errorCode = ErrorCode.PARAM_ERROR;
        }
        // 重复判断
        Broadcast<CoreConfig> confBroadcast = ConfBroadcast.getInstance(jssc.sparkContext(), false);
        CoreConfig coreConfig = confBroadcast.getValue();
        switch (type) {
            case CommonConstant.DATA_TYPE_TRACK:
                if (!StringUtil.isEmpty(coreConfig.getTrackTableNameSetting(uniqueId))) {
                    errorCode = ErrorCode.TRACK_UNIQUEID_IS_NULL;
                }
                break;
            case CommonConstant.DATA_TYPE_BINLOG:
                if (!StringUtil.isEmpty(coreConfig.getBinlogTableNameSetting(uniqueId))) {
                    errorCode = ErrorCode.BINLOG_UNIQUEID_IS_NULL;
                }
                break;
        }

        try {
            if (errorCode == 0) {

                String hdfsFile = HdfsPropFileUtil.getHdfsFile(type);
                if (hdfsFile != null) {
                    updateConf(hdfsFile, uniqueId, tableName);
                }
                response.sendRedirect("/conf");
            } else {
                response.sendRedirect("/settings?errorCode=" + errorCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized void addRowkeyConfAction(Request baseRequest, HttpServletRequest request,
                                                  HttpServletResponse response) {
        int errorCode = ErrorCode.SUCCESS;
        String rule = request.getParameter("rule").trim();
        String tableName = request.getParameter("tableName").trim();
        if (StringUtil.isEmpty(rule) || StringUtil.isEmpty(tableName)) {
            errorCode = ErrorCode.PARAM_ERROR;
        }
        // 重复判断
        Broadcast<CoreConfig> confBroadcast = ConfBroadcast.getInstance(jssc.sparkContext(), false);
        CoreConfig coreConfig = confBroadcast.getValue();
        if (coreConfig.getRowkeySetting(tableName) != null) {
            errorCode = ErrorCode.CONF_EXIST;
        }

        try {
            if (errorCode == 0) {

                String hdfsFile = HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_TYPE_ROWKEY);
                if (hdfsFile != null) {
                    updateConf(hdfsFile, tableName, rule);
                }
                response.sendRedirect("/conf");
            } else {
                response.sendRedirect("/settings?errorCode=" + errorCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除hbase、binlog和rowkey配置。不支持聚合索引配置删除
     * 
     * @param baseRequest
     * @param request
     * @param response
     */
    private synchronized void removeConfAction(Request baseRequest, HttpServletRequest request,
                                               HttpServletResponse response) {
        int errorCode = ErrorCode.SUCCESS;
        String uniqueId = request.getParameter("uniqueId").trim();
        String type = request.getParameter("type").trim();
        if (StringUtil.isEmpty(uniqueId) || StringUtil.isEmpty(type)) {
            errorCode = ErrorCode.PARAM_ERROR;
        }
        // 重复判断
        Broadcast<CoreConfig> confBroadcast = ConfBroadcast.getInstance(jssc.sparkContext(), false);
        CoreConfig coreConfig = confBroadcast.getValue();
        switch (type) {
            case CommonConstant.CONF_TYPE_TRACK:
                if (StringUtil.isEmpty(coreConfig.getTrackTableNameSetting(uniqueId))) {
                    errorCode = ErrorCode.TRACK_UNIQUEID_IS_NULL;
                }
                break;
            case CommonConstant.CONF_TYPE_BINLOG:
                if (StringUtil.isEmpty(coreConfig.getBinlogTableNameSetting(uniqueId))) {
                    errorCode = ErrorCode.BINLOG_UNIQUEID_IS_NULL;
                }
                break;
            case CommonConstant.CONF_TYPE_ROWKEY:
                if (coreConfig.getRowkeySetting(uniqueId) == null) {
                    errorCode = ErrorCode.ROWKEY_CONF_IS_NULL;
                }
                break;
        }

        try {
            if (errorCode == 0) {
                String hdfsFile = HdfsPropFileUtil.getHdfsFile(type);
                if (hdfsFile != null) {
                    HdfsPropFileUtil.removeConf(hdfsFile, uniqueId, new HdfsUtils());
                }
                // 更新广播变量
                ConfBroadcast.getInstance(jssc.sparkContext(), true);
                response.sendRedirect("/conf");
            } else {
                response.sendRedirect("/conf?errorCode=" + errorCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据聚合配置唯一UUID删除配置
     * 
     * @param baseRequest
     * @param request
     * @param response
     * @throws IOException
     */
    private synchronized void removeAggConfAction(Request baseRequest, HttpServletRequest request,
                                                  HttpServletResponse response) throws IOException {
        String uuid = request.getParameter("uuid");
        if (StringUtil.isEmpty(uuid)) {
            response.sendRedirect("/conf?errorCode=" + ErrorCode.PARAM_ERROR);
            return;
        }

        Broadcast<CoreConfig> confBroadcast = ConfBroadcast.getInstance(jssc.sparkContext(), false);
        CoreConfig coreConfig = confBroadcast.getValue();
        Map<String, List<AggConfig>> confMap = coreConfig.getAggSettings();
        if (confMap == null) {
            response.sendRedirect("/conf?errorCode=" + ErrorCode.AGG_CONF_NOT_EXIST);
            return;
        }

        String tableName = null;
        List<AggConfig> confs = null;
        for (Entry<String, List<AggConfig>> item : confMap.entrySet()) {
            if (tableName != null) {
                break;
            }
            confs = item.getValue();
            for (int i = 0; i < confs.size(); i++) {
                AggConfig aggConf = confs.get(i);
                if (aggConf.getUuid().equalsIgnoreCase(uuid)) {
                    confs.remove(i);
                    tableName = item.getKey();
                    break;
                }
            }
        }

        try {
            String hdfsFile = HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_TYPE_AGGREGATION);
            if (hdfsFile != null) {
                if (confs != null) {
                    updateConf(hdfsFile, tableName, new Gson().toJson(confs));
                } else {
                    updateConf(hdfsFile, tableName, null);
                }
            }
            // 更新广播变量
            ConfBroadcast.getInstance(jssc.sparkContext(), true);
            response.sendRedirect("/conf");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新配置
     * 
     * @param type
     * @param key
     * @param value
     * @throws IOException
     * @throws Exception
     */
    private void updateConf(String hdfsFile, String key, String value) throws IOException, Exception {

        HdfsPropFileUtil.updateConf(hdfsFile, key, value, new HdfsUtils());
        // 更新广播变量
        ConfBroadcast.getInstance(jssc.sparkContext(), true);
    }

    /**
     * 更新配置
     * 
     * @param baseRequest
     * @param request
     * @param response
     */
    private void refreshConfAction(Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        // 更新广播变量
        ConfBroadcast.getInstance(jssc.sparkContext(), true);
        try {
            response.sendRedirect("/conf");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized void addAggConfAction(Request baseRequest, HttpServletRequest request,
                                               HttpServletResponse response) throws IOException {
        int errorCode = ErrorCode.SUCCESS;
        String tableName = request.getParameter("tableName").trim();
        String sourceName = request.getParameter("sourceName").trim();
        String group = request.getParameter("group").trim();
        String agg = request.getParameter("agg").trim();
        String sink = request.getParameter("sink").trim();
        String segmentGranularity = request.getParameter("segmentGranularity").trim();
        if (StringUtil.isEmpty(tableName) || StringUtil.isEmpty(sourceName) || StringUtil.isEmpty(group)
            || StringUtil.isEmpty(agg) || StringUtil.isEmpty(sink)) {
            errorCode = ErrorCode.PARAM_ERROR;
            response.sendRedirect("/settings?errorCode=" + errorCode);
            return;
        }

        Broadcast<CoreConfig> confBroadcast = ConfBroadcast.getInstance(jssc.sparkContext(), false);
        CoreConfig coreConfig = confBroadcast.getValue();
        List<AggConfig> confs = coreConfig.getAggSetting(tableName);
        if (confs == null) confs = new ArrayList<>();

        AggConfig conf = new AggConfig();
        conf.setSourceName(sourceName);
        conf.setGroup(group);
        switch (sink) {
            case "hbase":
                conf.setSink(SinkEnum.hbase);
                break;
            case "druid":
                conf.setSink(SinkEnum.druid);
                break;
            default:
                errorCode = ErrorCode.PARAM_ERROR;
                response.sendRedirect("/settings?errorCode=" + errorCode);
                return;
        }

        conf.setAgg(agg);
        conf.setSegmentGranularity(SegmentGranularityEnum.get(segmentGranularity));
        confs.add(conf);

        try {
            String hdfsFile = HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_TYPE_AGGREGATION);
            if (hdfsFile != null) {
                updateConf(hdfsFile, tableName, new Gson().toJson(confs));
            }
            response.sendRedirect("/conf");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 更新告警超时时间
     * 
     * @param baseRequest
     * @param request
     * @param response
     * @throws IOException
     */
    private synchronized void updateAlarmsTimeoutAction(Request baseRequest, HttpServletRequest request,
                                                        HttpServletResponse response) throws IOException {
        String conf_name = request.getParameter("conf_name").trim();
        String conf_type = request.getParameter("conf_type").trim();
        String timeout = request.getParameter("timeout").trim();

        if (StringUtil.isEmpty(conf_name) || StringUtil.isEmpty(conf_type) || StringUtil.isEmpty(timeout)) {
            response.sendRedirect("/alarms?errorCode=" + ErrorCode.PARAM_ERROR);
            return;
        }

        Pattern pattern = Pattern.compile("[0-9]{1,}");
        Matcher matcher = pattern.matcher(timeout);
        boolean result = matcher.matches();
        if (!result) {
            response.sendRedirect("/alarms?errorCode=" + ErrorCode.PARAM_ERROR);
            return;
        }

        try {
            String hdfsFile = HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_ALARMS);
            if (hdfsFile != null) {
                HdfsPropFileUtil.updateConf(hdfsFile, conf_name + "/" + conf_type, timeout, new HdfsUtils());
            }

            // 更新广播变量
            ConfBroadcast.getInstance(jssc.sparkContext(), true);

            response.sendRedirect("/alarms");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private synchronized void updateAlarmsNotifyConf(Request baseRequest, HttpServletRequest request,
                                                     HttpServletResponse response) throws IOException {
        String postUrl = request.getParameter("postUrl").trim();
        String body = request.getParameter("body").trim();

        if (StringUtil.isEmpty(postUrl) || StringUtil.isEmpty(body)) {
            response.sendRedirect("/settings?errorCode=" + ErrorCode.PARAM_ERROR);
            return;
        }

        Pattern pattern = Pattern.compile("^([hH][tT]{2}[pP]:/*|[hH][tT]{2}[pP][sS]:/*|[fF][tT][pP]:/*)(([A-Za-z0-9-~]+).)+([A-Za-z0-9-~\\\\/])+(\\\\?{0,1}(([A-Za-z0-9-~]+\\\\={0,1})([A-Za-z0-9-~]*)\\\\&{0,1})*)$");
        Matcher matcher = pattern.matcher(postUrl);
        boolean result = matcher.matches();
        if (!result) {
            response.sendRedirect("/settings?errorCode=" + ErrorCode.PARAM_ERROR);
            return;
        }

        try {
            String hdfsFile = HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_ALARM_NOTIFICATION);
            if (hdfsFile != null) {
                HdfsUtils hdfsUtils = new HdfsUtils();
                HdfsPropFileUtil.updateConf(hdfsFile, CommonConstant.NOTIFY_CONF_POST_URL, postUrl, hdfsUtils);
                HdfsPropFileUtil.updateConf(hdfsFile, CommonConstant.NOTIFY_CONF_BODY, body, hdfsUtils);
            }

            // 更新广播变量
            ConfBroadcast.getInstance(jssc.sparkContext(), true);

            response.sendRedirect("/settings");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 追踪Druid任务
     * 
     * @param jsc
     * @throws Exception
     */
    private static void trackDruidTask(JavaSparkContext jsc) throws Exception {
        Broadcast<HdfsUtils> hdfsUtilsBroadcast = HdfsUtilBroadcast.getInstance(jsc);
        HdfsUtils hdfsUtils = hdfsUtilsBroadcast.getValue();
        // 扫描日志目录，获取所有process结尾的日志文件
        List<String> logFiles = hdfsUtils.listFile(ConfProperties.getStringValue(ConfigConstant.druid_agg_data_push_log_dir));
        for (String item : logFiles) {
            if (item.endsWith("process")) {
                // 反序列化
                String druidTaskInfoJson = new String(hdfsUtils.readHDFSFile(item));
                DruidTaskInfo druidTaskInfo = new Gson().fromJson(druidTaskInfoJson, DruidTaskInfo.class);
                // 获取taskId
                String taskId = druidTaskInfo.getTaskId();
                // 通过文件名中的taskid获取最新状态
                String status = DruidUtils.getTaskStatus(taskId);
                // 状态回写
                druidTaskInfo.setStatus(status);
                hdfsUtils.createNewHDFSFile(item, druidTaskInfo.toString());
                // 如果状态是success/failed，则去掉process后缀
                if (status.trim().toLowerCase().equals("success") || status.trim().toLowerCase().equals("failed")) {
                    hdfsUtils.move(item, item.replace(".process", ""));
                }
            }
        }
    }

    private static void cleanDruidTaskLog(JavaSparkContext jsc) throws Exception {
        Broadcast<HdfsUtils> hdfsUtilsBroadcast = HdfsUtilBroadcast.getInstance(jsc);
        HdfsUtils hdfsUtils = hdfsUtilsBroadcast.getValue();
        FileStatus[] files = hdfsUtils.listFileStatus(ConfProperties.getStringValue(ConfigConstant.druid_agg_data_push_log_dir));
        if (files != null && files.length > 0) {
            for (FileStatus file : files) {
                // 忽略目录
                if (!file.isFile()) {
                    continue;
                }
                String fileName = file.getPath().getName();
                long modificationTime = file.getModificationTime();
                // 忽略正在跟踪中的文件
                if (fileName.endsWith("process")) {
                    continue;
                }
                // 处理Log文件
                if (fileName.endsWith("log")) {
                    // 反序列化
                    String druidTaskInfoJson = new String(hdfsUtils.readHDFSFile(file.getPath().toString()));
                    DruidTaskInfo druidTaskInfo = new Gson().fromJson(druidTaskInfoJson, DruidTaskInfo.class);
                    if (druidTaskInfo.getStatus().trim().equalsIgnoreCase("success")
                        // 删除超过24小时并且成功推送到Druid的日志文件
                        && (System.currentTimeMillis() - modificationTime >= (24 * 60 * 60 * 1000))) {
                        hdfsUtils.deleteHDFSFile(file.getPath().toString());
                    } else if (System.currentTimeMillis() - modificationTime >= (7 * 24 * 60 * 60 * 1000)) {
                        // 删除超过7*24小时推送失败的日志文件
                        hdfsUtils.deleteHDFSFile(file.getPath().toString());
                    }

                }
                // 处理状态是Success的err文件
                if (fileName.endsWith("err")
                    && (System.currentTimeMillis() - modificationTime >= (7 * 24 * 60 * 60 * 1000))) {
                    hdfsUtils.deleteHDFSFile(file.getPath().toString());
                }

            }
        }
    }

    /**
     * 推送数据到druid
     * 
     * @param jsc
     * @throws IOException
     */
    private static void pushDataToDriud(JavaSparkContext jsc,
                                        Map<String, List<AggConfig>> aggConfMap) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);
        System.out.println("检查是否需要推送数据文本到Druid " + format.format(new Date(System.currentTimeMillis())));

        Broadcast<HdfsUtils> hdfsUtilsBroadcast = HdfsUtilBroadcast.getInstance(jsc);
        HdfsUtils hdfsUtils = hdfsUtilsBroadcast.getValue();

        // 处理待提交的Druid数据文件
        Broadcast<List<String>> untreated = FlushDruidTaskBroadcast.getInstance(jsc, hdfsUtils);
        if (untreated == null || untreated.getValue().size() == 0) return;

        // 提交untreated到Druid,提交之后释放untreated
        List<String> untreatedFileList = untreated.getValue();
        if (untreatedFileList != null && untreatedFileList.size() > 0) {

            for (String path : untreatedFileList) {
                String fullFileName = DruidUtils.getDataFileFullName(path);
                // 构建dts路径。处理完的文件会放入druid_pushed目录中
                String dstPath = ConfProperties.getStringValue(ConfigConstant.druid_agg_data_pushed_dir) + "/"
                                 + fullFileName;

                // 解析path获取文件的sourceName
                String sourceName = DruidUtils.getSourceNameForFile(path);

                // 通过source查找聚合配置
                AggConfig aggconf = findAggConfig(aggConfMap, sourceName, SinkEnum.druid);
                DruidTaskInfo druidTaskInfo = new DruidTaskInfo(dstPath);
                if (aggconf == null) {// 查找不到配置
                    druidTaskInfo.setError(true);
                    druidTaskInfo.setErrorMessage("Configuration not found");
                    // 已经处理过的文件移动到druid_pushed目录中并记录日志
                    saveDruidPushLog(druidTaskInfo, hdfsUtils);
                    continue;
                }

                // 通过配置判断当前数据文件中包含的时间粒度是否符合推送要求
                if (path.endsWith(DruidUtils.getDruidDataFileName(aggconf.getSourceName(),
                                                                  aggconf.getSegmentGranularity()))) {
                    continue;
                }

                System.out.println("以下文件将会推送到Druid：" + dstPath);

                // 把文件移动到已处理的目录
                hdfsUtils.move(path, dstPath);

                // 构建druid配置文件
                String confStr = createDruidConf(sourceName, dstPath, aggconf);

                // 通过http提交推送
                String taskId = DruidUtils.sendDataDruid(confStr);

                if (taskId != null) {
                    // 获取任务ID 更改任务状态
                    druidTaskInfo.setTaskId(taskId);
                    druidTaskInfo.setStatus("pending");
                } else {
                    druidTaskInfo.setError(true);
                    druidTaskInfo.setErrorMessage("send error \n push info:" + confStr);
                }

                // 已经处理过的文件移动到druid_pushed目录中并记录日志
                saveDruidPushLog(druidTaskInfo, hdfsUtils);
            }

            // 释放
            FlushDruidTaskBroadcast.clean();
        }
    }

    /**
     * 保存Druid推送日志到hdfs
     * 
     * @param druidTaskInfo
     * @param hdfsUtils
     * @throws Exception
     */
    private static void saveDruidPushLog(DruidTaskInfo druidTaskInfo, HdfsUtils hdfsUtils) throws Exception {
        String logFileName = null;
        // 构建日志文件名称
        // 正常发送的文件名称结构是：task_id.log.process/task_id.log
        // 发送失败的文件名称结构是：数据文件名称.err
        // 只有process结尾的日志文件需要跟踪任务状态
        if (druidTaskInfo.isError()) {
            logFileName = DruidUtils.getDataFileName(druidTaskInfo.getPath()) + ".err";
        } else {
            logFileName = DruidUtils.getDataFileName(druidTaskInfo.getPath()) + ".log.process";
        }

        String logFilePath = ConfProperties.getStringValue(ConfigConstant.druid_agg_data_push_log_dir) + "/"
                             + logFileName;
        if (!hdfsUtils.exist(logFilePath)) {
            hdfsUtils.createNewHDFSFile(logFilePath, druidTaskInfo.toString());
        }

    }

    private static String getInterval(SegmentGranularityEnum segmentGranularityEnum, Date startDate) {
        SimpleDateFormat format = null;
        DateTimeFormatter dateTimeFormatter = null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        String endDateStr = null;
        switch (segmentGranularityEnum) {
            case year:
                format = new SimpleDateFormat(CommonConstant.YYYY);
                calendar.setTime(startDate);
                calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) + 1);
                endDateStr = format.format(calendar.getTime());
                return format.format(startDate) + "/" + endDateStr;
            case month:
                format = new SimpleDateFormat(CommonConstant.YYYY_MM);
                calendar.setTime(startDate);
                calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) + 1);
                endDateStr = format.format(calendar.getTime());
                return format.format(startDate) + "/" + endDateStr;
            case day:
                format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD);
                calendar.setTime(startDate);
                calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) + 1);
                endDateStr = format.format(calendar.getTime());
                return format.format(startDate) + "/" + endDateStr;
            case hour:
                format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH);
                calendar.setTime(startDate);
                calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) + 1);
                endDateStr = format.format(calendar.getTime());
                dateTimeFormatter = DateTimeFormatter.ofPattern(format.toPattern());
                return LocalDateTime.parse(format.format(startDate), dateTimeFormatter) + "/"
                       + LocalDateTime.parse(endDateStr, dateTimeFormatter);
            case minute:
                format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM);
                calendar.setTime(startDate);
                calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) + 1);
                endDateStr = format.format(calendar.getTime());
                dateTimeFormatter = DateTimeFormatter.ofPattern(format.toPattern());
                return LocalDateTime.parse(format.format(startDate), dateTimeFormatter) + "/"
                       + LocalDateTime.parse(endDateStr, dateTimeFormatter);
            default:
                return "";
        }

    }

    private static String createDruidConf(String sourceName, String path, AggConfig aggconf) throws ParseException {
        Date startDate = DruidUtils.getFileInterval(path, aggconf.getSegmentGranularity());// 文件的后缀描述了数据属于那天

        HdfsParam hdfsParam = new HdfsParam();
        hdfsParam.setDataSourceName(sourceName);
        hdfsParam.setSegmentGranularity(aggconf.getSegmentGranularity());
        hdfsParam.setInterval(getInterval(aggconf.getSegmentGranularity(), startDate));
        hdfsParam.setPartitionsType("hashed");
        hdfsParam.setPaths(path);
        hdfsParam.setTargetPartitionSize(5000000);

        List<String> dimensions = new ArrayList<>();
        for (String field : aggconf.getGroupFields()) {
            if (field.startsWith(FunctionNameConstant.TIME_FORMAT)) continue;
            dimensions.add(field);
        }

        String[] dimensionArray = new String[dimensions.size()];
        dimensions.toArray(dimensionArray);
        hdfsParam.setDimensions(dimensionArray);

        List<MetricsSpec> metricsSpecs = new ArrayList<>();
        for (String columnDefinition : aggconf.getAggFields()) {
            MetricsSpec metricsSpec = null;
            AggregationFieldEntity column = DruidUtils.analysisColumnDefinition(columnDefinition);
            switch (column.getOperator()) {
                case MIN:
                    metricsSpec = new MetricsSpec(MetricsSpecTypeEnum.doubleMin, column.getAggFieldName(),
                                                  column.getAggFieldName());
                    break;
                case MAX:
                    metricsSpec = new MetricsSpec(MetricsSpecTypeEnum.doubleMax, column.getAggFieldName(),
                                                  column.getAggFieldName());
                    break;
                case SUM:
                    metricsSpec = new MetricsSpec(MetricsSpecTypeEnum.doubleSum, column.getAggFieldName(),
                                                  column.getAggFieldName());
                    break;

                default:
                    metricsSpec = new MetricsSpec(MetricsSpecTypeEnum.doubleSum, column.getAggFieldName(),
                                                  column.getAggFieldName());// 如果是count可能没有field字段
                    break;
            }

            metricsSpecs.add(metricsSpec);
        }

        MetricsSpec[] metricsSpecArray = new MetricsSpec[metricsSpecs.size()];
        metricsSpecs.toArray(metricsSpecArray);
        hdfsParam.setMetricsSpecs(metricsSpecArray);
        return DruidConfig.createConfToHdfs(hdfsParam);
    }

    /**
     * 根据sourceName查找聚合配置
     * 
     * @param aggConf
     * @param sourceName
     * @return
     */
    private static AggConfig findAggConfig(Map<String, List<AggConfig>> aggConf, String sourceName, SinkEnum sink) {

        for (Entry<String, List<AggConfig>> entry : aggConf.entrySet()) {
            for (AggConfig item : entry.getValue()) {
                if (item.getSourceName().equalsIgnoreCase(sourceName) && item.getSink().equals(sink)) {
                    return item;
                }
            }
        }
        return null;
    }

    public void scanAlarms(ConcurrentHashMap<String, MonitorValue> saveHistory, Map<String, String> trackConfigs,
                           Map<String, String> binlogConfigs, Map<String, Long> alarmConfig, String postUrl,
                           String body) {
        if (StringUtil.isEmpty(postUrl) || StringUtil.isEmpty(body)) {
            return;
        }

        MonitorUIConfig uiConfig = new MonitorUIConfig(sc);
        // 如果应用的启动时间少于10分钟则不进行任何告警检查
        if ((System.currentTimeMillis() - uiConfig.getAppStartTime().getTime()) < (1000 * 60 * 10)) {
            return;
        }

        // 分别扫描track和binlog
        for (Entry<String, String> item : trackConfigs.entrySet()) {
            Long timeout = getTimeout(alarmConfig, item.getKey() + "/track");
            long checkResult = notifyCheck(timeout, saveHistory.get(item.getKey()), uiConfig.getAppStartTime());
            if (checkResult != -1) {
                StringBuilder content = new StringBuilder(Env.getEnvironment() + "环境");
                content.append("Track监控报警：");
                content.append(item.getKey());
                content.append("已经超过");
                content.append((checkResult / 1000 / 60));
                content.append("Minute未收到数据.");
                content.append("阈值：");
                content.append(timeout);
                content.append("Minute");
                System.out.println(content.toString());
                HttpClientUtil.httpPostRaw(postUrl, body.replace("${content}", content.toString()), null, "utf-8", 3000,
                                           1000, 3000);
            }
        }

        for (Entry<String, String> item : binlogConfigs.entrySet()) {
            Long timeout = getTimeout(alarmConfig, item.getKey() + "/binlog");
            long checkResult = notifyCheck(timeout, saveHistory.get(item.getKey()), uiConfig.getAppStartTime());
            if (checkResult != -1) {
                StringBuilder content = new StringBuilder(Env.getEnvironment() + "环境");
                content.append("Binlog监控报警：");
                content.append(item.getKey());
                content.append("已经超过");
                content.append((checkResult / 1000 / 60));
                content.append("Minute未收到数据.");
                content.append("阈值：");
                content.append(timeout);
                content.append("Minute");
                System.out.println(content.toString());
                HttpClientUtil.httpPostRaw(postUrl, body.replace("${content}", content.toString()), null, "utf-8", 3000,
                                           1000, 3000);
            }
        }

    }

    /**
     * 检查是否需要发送通知<br>
     * 如果返回值等于-1则不需要通知。反之则代表超时时间（毫秒值）
     * 
     * @param timeout
     * @param monitorValue
     * @param appStartTime
     * @return
     */
    private long notifyCheck(Long timeout, MonitorValue monitorValue, Date appStartTime) {
        if (timeout == 0) {
            return -1;
        }

        // 计算某个在配置中的表最近数据进来的时间，如果没有则从启动时间算起.和当前时间比，如果时间超过一定时长，则是红色。
        Date lastOpTime = null;
        if (monitorValue != null) {
            lastOpTime = monitorValue.getLastOpTime();
        } else {
            lastOpTime = appStartTime;
        }

        long diff = System.currentTimeMillis() - lastOpTime.getTime();
        if (diff > (timeout * 60 * 1000)) {
            return diff;
        }
        return -1;
    }

    public Long getTimeout(Map<String, Long> alarmConfig, String confName) {
        Long result = alarmConfig.get(confName);
        if (result != null) {
            return result;
        }
        return 0l;
    }

    private void print(String msg, HttpServletResponse response) {
        PrintWriter out = null;
        try {
            out = response.getWriter();
            out.println(msg);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

}
