package cn.tsign.ui.tab;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.api.java.JavaSparkContext;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.spark.broadcast.entity.CoreConfig.AggConfig;
import cn.tsign.ui.TemplateAbstract;
import cn.tsign.ui.entity.AggMonitorValue;
import cn.tsign.ui.entity.MonitorValue;

public class ConfTabUI extends TemplateAbstract {

    private CoreConfig                         coreConfig;

    ConcurrentHashMap<String, MonitorValue>    saveHistoryMap;

    ConcurrentHashMap<String, AggMonitorValue> aggHistoryMap = new ConcurrentHashMap<>();

    public ConfTabUI(JavaSparkContext sc, CoreConfig coreConfig, ConcurrentHashMap<String, MonitorValue> saveHistoryMap,
                     ConcurrentHashMap<String, AggMonitorValue> aggHistoryMap){
        super(sc);
        this.coreConfig = coreConfig;
        this.saveHistoryMap = saveHistoryMap;
        this.aggHistoryMap = aggHistoryMap;
    }

    private static final long serialVersionUID = 2648395994692714502L;

    @Override
    public String bulid(String... appendElements) {
        StringBuilder html = new StringBuilder("<div class='container-fluid'>");
        html.append(alertView());
        html.append(head());

        html.append("<span>");
        html.append(trackConfTable());
        html.append(binlogConfTable());
        html.append(rowkeyConfTable());
        html.append(aggConfTable());
        html.append("</span>");

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }

        return html.append("</div>").toString();
    }

    public String head() {
        StringBuilder head = new StringBuilder("<div class='row-fluid'><div class='span12'>");
        head.append("<h3 style='vertical-align: bottom; display: inline-block;'>");
        head.append("Config");
        head.append("</h3>");
        return head.append("</div></div>").toString();
    }

    public String trackConfTable() {
        StringBuilder html = new StringBuilder("<h4>Track Config</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='40.0%' class=''>Unique Id <i style='font-size: xx-small;'>[cid]+[event]</i></th>");
        html.append("<th width='30.0%' class=''>Hbase Table</th>");
        html.append("<th width='20.0%' class=''>Last Save Time</th>");
        html.append("<th width='10.0%' class=''>Remove</th>");
        html.append("</tr></thead>");

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);

        html.append("<tbody>");
        for (Entry<String, String> item : coreConfig.getTrackTableNameSettings().entrySet()) {
            Long timeout = getTimeout(item.getKey() + "/track");

            html.append("<tr " + getThAttr(item.getKey(), timeout) + " >");
            html.append("<td>" + item.getKey() + "</td>");
            html.append("<td>" + item.getValue() + "</td>");
            html.append("<td>" + getSaveHistoryLastSaveTime(item.getKey(), format) + "</td>");
            html.append("<td><button type='button' class='btn btn-inverse' onclick=\"javascript:window.location.href='/removeConfAction?type=track&uniqueId="
                        + item.getKey() + "'\" >Del</button></td>");
            html.append("</tr>");
        }
        html.append("</tbody>");

        return html.append("<tfoot></tfoot></table>").toString();
    }

    public String binlogConfTable() {
        StringBuilder html = new StringBuilder("<h4>Binlog Config</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='40.0%' class=''>Unique Id <i style='font-size: xx-small;'>[database]+[table]</i></th>");
        html.append("<th width='30.0%' class=''>Hbase Table</th>");
        html.append("<th width='20.0%' class=''>Last Save Time</th>");
        html.append("<th width='10.0%' class=''>Remove</th>");
        html.append("</tr></thead>");

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);

        html.append("<tbody>");
        for (Entry<String, String> item : coreConfig.getBinlogTableNameSettings().entrySet()) {
            Long timeout = getTimeout(item.getKey() + "/binlog");

            html.append("<tr " + getThAttr(item.getKey(), timeout) + " >");
            html.append("<td>" + item.getKey() + "</td>");
            html.append("<td>" + item.getValue() + "</td>");
            html.append("<td>" + getSaveHistoryLastSaveTime(item.getKey(), format) + "</td>");
            html.append("<td><button type='button' class='btn btn-inverse' onclick=\"javascript:window.location.href='/removeConfAction?type=binlog&uniqueId="
                        + item.getKey() + "'\" >Del</button></td>");
            html.append("</tr>");
        }
        html.append("</tbody>");

        return html.append("<tfoot></tfoot></table>").toString();
    }

    private String getSaveHistoryLastSaveTime(String hbaseTable, SimpleDateFormat format) {
        MonitorValue monitorValue = saveHistoryMap.get(hbaseTable);
        if (monitorValue != null) {
            return format.format(monitorValue.getLastOpTime());
        }
        return "--";
    }

    private String getAggHistoryLastSaveTime(String confUUid, SimpleDateFormat format) {
        AggMonitorValue monitorValue = aggHistoryMap.get(confUUid);
        if (monitorValue != null) {
            return format.format(monitorValue.getLastOpTime());
        }
        return "--";
    }

    public Long getTimeout(String confName) {
        Long result = coreConfig.getAlarmConfig(confName);
        if (result != null) {
            return result;
        }
        return 0l;
    }

    private String getThAttr(String hbaseTable, Long timeout) {
        if (timeout == 0) {
            return "";
        }
        // 如果应用的启动时间少于10分钟则不进行任何告警检查
        if ((System.currentTimeMillis() - this.getUiConf().getAppStartTime().getTime()) < (1000 * 60 * 10)) {
            return "";
        }
        // 计算某个在配置中的表最近数据进来的时间，如果没有则从启动时间算起.和当前时间比，如果时间超过一定时长，则是红色。
        Date lastOpTime = null;
        MonitorValue monitorValue = saveHistoryMap.get(hbaseTable);
        if (monitorValue != null) {
            lastOpTime = monitorValue.getLastOpTime();
        } else {
            lastOpTime = this.getUiConf().getAppStartTime();
        }

        long diff = System.currentTimeMillis() - lastOpTime.getTime();
        if (diff > (timeout * 1000 * 60)) {
            return " color='red' style='color:red;font-weight:bold;font-style:italic;' title='Finally, the storage time exceeded 10 minutes and reached "
                   + (diff / 1000 / 60) + " minutes' ";
        }
        return "";
    }

    public String rowkeyConfTable() {
        StringBuilder html = new StringBuilder("<h4>Rowkey Config</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='50.0%' class=''>Hbase Table</th>");
        html.append("<th width='40.0%' class=''>Rule</th>");
        html.append("<th width='10.0%' class=''>Remove</th>");
        html.append("</tr></thead>");

        html.append("<tbody>");
        for (Entry<String, String> item : coreConfig.getRowkeySettings().entrySet()) {
            html.append("<tr>");
            html.append("<td>" + item.getKey() + "</td>");
            html.append("<td>" + item.getValue() + "</td>");
            html.append("<td><button type='button' class='btn btn-inverse' onclick=\"javascript:window.location.href='/removeConfAction?type=rowkey&uniqueId="
                        + item.getKey() + "'\" >Del</button></td>");
            html.append("</tr>");
        }
        html.append("</tbody>");

        return html.append("<tfoot></tfoot></table>").toString();

    }

    public String aggConfTable() {
        StringBuilder html = new StringBuilder("<h4>Aggregation Config</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='10.0%' class=''>Hbase Table</th>");
        html.append("<th width='5.0%' class=''>Sink</th>");
        html.append("<th width='10.0%' class=''>Granularity</th>");
        html.append("<th width='15.0%' class=''>SourceName</th>");
        html.append("<th width='35.0%' class=''>Group&Agg</th>");
        html.append("<th width='15.0%' class=''>Last Save Time</th>");
        html.append("<th width='10.0%' class=''>Remove</th>");
        html.append("</tr></thead>");

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);

        html.append("<tbody>");
        for (Entry<String, List<AggConfig>> item : coreConfig.getAggSettings().entrySet()) {
            for (AggConfig aggconf : item.getValue()) {
                html.append("<tr>");
                html.append("<td >" + item.getKey() + "</td>");
                html.append("<td >" + aggconf.getSink() + "</td>");
                html.append("<td >" + aggconf.getSegmentGranularity().name() + "</td>");
                html.append("<td >" + aggconf.getSourceName() + "</td>");
                html.append("<td >" + "<font style='font-weight: bold;'>Group:</font>" + aggconf.getGroup() + "</br>"
                            + "<font style='font-weight: bold;'>Agg:</font>" + aggconf.getAgg() + "</td>");
                html.append("<td >" + getAggHistoryLastSaveTime(aggconf.getUuid(), format) + "</td>");
                html.append("<td><button type='button' class='btn btn-inverse' onclick=\"javascript:window.location.href='/removeAggConfAction?uuid="
                            + aggconf.getUuid() + "'\" >Del</button></td>");
                html.append("</tr>");
            }
        }
        html.append("</tbody>");

        return html.append("<tfoot></tfoot></table>").toString();
    }

}
