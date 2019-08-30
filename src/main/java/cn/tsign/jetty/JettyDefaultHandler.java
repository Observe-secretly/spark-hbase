package cn.tsign.jetty;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.Gson;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.constant.SinkEnum;
import cn.tsign.common.hbase.HbaseUtil;
import cn.tsign.common.util.HdfsPropFileUtil;
import cn.tsign.common.util.HdfsUtils;
import cn.tsign.common.util.StringUtil;
import cn.tsign.spark.accumulator.AggMonitorAccumulator;
import cn.tsign.spark.accumulator.MonitorAccumulator;
import cn.tsign.spark.broadcast.ConfBroadcast;
import cn.tsign.spark.broadcast.HbaseClientBroadcast;
import cn.tsign.spark.broadcast.HdfsUtilBroadcast;
import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.spark.broadcast.entity.CoreConfig.AggConfig;
import cn.tsign.ui.BodyTemplate;
import cn.tsign.ui.HeadTemplate;
import cn.tsign.ui.js.AlertJs;
import cn.tsign.ui.js.BlockJs;
import cn.tsign.ui.tab.ConfTabUI;
import cn.tsign.ui.tab.DruidPushTabUI;
import cn.tsign.ui.tab.MonitorNetworkTabUI;
import cn.tsign.ui.tab.MonitorTabUI;
import cn.tsign.ui.tab.OperatorTabUI;
import cn.tsign.ui.tab.SettingsTabUI;

public class JettyDefaultHandler extends AbstractHandler {

    JavaSparkContext      sc;
    JavaStreamingContext  jssc;
    Server                server;
    MonitorAccumulator    monitorAccumulator;
    AggMonitorAccumulator aggMonitorAccumulator;

    public JettyDefaultHandler(){
    }

    public JettyDefaultHandler(JavaSparkContext sc, JavaStreamingContext jssc, MonitorAccumulator monitorAccumulator,
                               AggMonitorAccumulator aggMonitorAccumulator, Server server){
        this.sc = sc;
        this.jssc = jssc;
        this.monitorAccumulator = monitorAccumulator;
        this.aggMonitorAccumulator = aggMonitorAccumulator;
        this.server = server;
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
            case _cat_config_druid_push:
                body = CatApiHandler.catConfigDruidPush(ConfBroadcast.getInstance(jssc.sparkContext(),
                                                                                  false).getValue(),
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
        body.appendElement(new SettingsTabUI(sc, hbaseUtilBroadcast.getValue().getListTableNames()).toTemplate());

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
        String uniqueId = request.getParameter("uniqueId");
        String type = request.getParameter("type");
        String tableName = request.getParameter("tableName");
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
        String rule = request.getParameter("rule");
        String tableName = request.getParameter("tableName");
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
        String uniqueId = request.getParameter("uniqueId");
        String type = request.getParameter("type");
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
        String tableName = request.getParameter("tableName");
        String sourceName = request.getParameter("sourceName");
        String group = request.getParameter("group");
        String agg = request.getParameter("agg");
        String sink = request.getParameter("sink");
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
