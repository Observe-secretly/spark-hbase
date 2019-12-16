package cn.tsign.ui.tab;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.api.java.JavaSparkContext;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.ui.TemplateAbstract;
import cn.tsign.ui.entity.MonitorValue;

public class AlarmsTabUI extends TemplateAbstract {

    private static final long                       serialVersionUID = -5549643684450479287L;

    private CoreConfig                              coreConfig;

    private ConcurrentHashMap<String, MonitorValue> saveHistoryMap;

    public AlarmsTabUI(JavaSparkContext sc, CoreConfig coreConfig,
                       ConcurrentHashMap<String, MonitorValue> saveHistoryMap){
        super(sc);

        this.coreConfig = coreConfig;
        this.saveHistoryMap = saveHistoryMap;
    }

    public String head() {
        StringBuilder head = new StringBuilder("<div class='row-fluid'><div class='span12'>");
        head.append("<h3 style='vertical-align: bottom; display: inline-block;'>");
        head.append("Alarms");
        head.append("</h3>");
        return head.append("</div></div>").toString();
    }

    public Long getTimeout(String confName) {
        Long result = coreConfig.getAlarmConfig(confName);
        if (result != null) {
            return result;
        }
        return 0l;
    }

    public String trackConfTable() {
        StringBuilder html = new StringBuilder("<h4>Track Config</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='35.0%' class=''>Unique Id <i style='font-size: xx-small;'>[cid]+[event]</i></th>");
        html.append("<th width='20.0%' class=''>Hbase Table</th>");
        html.append("<th width='20.0%' class=''>Last Save Time</th>");
        html.append("<th width='15.0%' class=''>Request Timeout <i style='font-size: xx-small;'>Minute<i></th>");
        html.append("<th width='10.0%' class=''>Operator</th>");
        html.append("</tr></thead>");

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);

        html.append("<tbody>");
        int index = 1;
        for (Entry<String, String> item : coreConfig.getTrackTableNameSettings().entrySet()) {
            Long timeout = getTimeout(item.getKey() + "/track");

            html.append("<tr " + getThAttr(item.getKey(), timeout) + " >");
            html.append("<td>" + item.getKey() + "</td>");
            html.append("<td>" + item.getValue() + "</td>");
            html.append("<td>" + getSaveHistoryLastSaveTime(item.getKey(), format) + "</td>");// TODO
                                                                                              // 需要根据配置的RequestTimeout进行告警
            html.append("<td><input name='track_" + index + "' type='text' conf_type='track' conf_name='"
                        + item.getKey() + "' value='" + timeout
                        + "'  style='height: 100%;width:100%;'  placeholder=''></td>");
            html.append("<td><button name='track_" + index
                        + "' op=true type='button' class='btn btn-inverse' onclick='' >Update</button></td>");
            html.append("</tr>");
            index++;
        }
        html.append("</tbody>");

        return html.append("<tfoot></tfoot></table>").toString();
    }

    public String binlogConfTable() {
        StringBuilder html = new StringBuilder("<h4>Binlog Config</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='35.0%' class=''>Unique Id <i style='font-size: xx-small;'>[database]+[table]</i></th>");
        html.append("<th width='20.0%' class=''>Hbase Table</th>");
        html.append("<th width='20.0%' class=''>Last Save Time</th>");
        html.append("<th width='15.0%' class=''>Request Timeout  <i style='font-size: xx-small;'>Minute<i></th>");
        html.append("<th width='10.0%' class=''>Operator</th>");
        html.append("</tr></thead>");

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);

        int index = 1;
        html.append("<tbody>");
        for (Entry<String, String> item : coreConfig.getBinlogTableNameSettings().entrySet()) {
            Long timeout = getTimeout(item.getKey() + "/binlog");

            html.append("<tr " + getThAttr(item.getKey(), timeout) + " >");
            html.append("<td>" + item.getKey() + "</td>");
            html.append("<td>" + item.getValue() + "</td>");
            html.append("<td>" + getSaveHistoryLastSaveTime(item.getKey(), format) + "</td>");

            html.append("<td><input name='binlog_" + index + "' type='text' conf_type='binlog' conf_name='"
                        + item.getKey() + "'  value='" + timeout
                        + "'  style='height: 100%;width:100%;'  placeholder=''></td>");
            html.append("<td><button name='binlog_" + index
                        + "' op=true type='button' class='btn btn-inverse' onclick='' >Update</button></td>");
            html.append("</tr>");
            html.append("</tr>");
            index++;
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

    @Override
    public String bulid(String... appendElements) {
        StringBuilder html = new StringBuilder("<div class='container-fluid'>");
        html.append(alertView());
        html.append(head());

        html.append("<span>");
        html.append(trackConfTable());
        html.append(binlogConfTable());
        html.append("</span>");

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }

        return html.append("</div>").toString();
    }

}
