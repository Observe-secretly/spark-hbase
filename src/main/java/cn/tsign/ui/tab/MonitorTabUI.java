package cn.tsign.ui.tab;

import java.text.SimpleDateFormat;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.api.java.JavaSparkContext;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.ui.TemplateAbstract;
import cn.tsign.ui.entity.AggMonitorValue;
import cn.tsign.ui.entity.MonitorValue;

public class MonitorTabUI extends TemplateAbstract {

    private static final long                          serialVersionUID  = -6834932668511729554L;

    private ConcurrentHashMap<String, MonitorValue>    saveHistoryMap    = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, AggMonitorValue> aggHistoryMap     = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, MonitorValue>    discardHistoryMap = new ConcurrentHashMap<>();

    public MonitorTabUI(JavaSparkContext sc, ConcurrentHashMap<String, MonitorValue> saveHistoryMap,
                        ConcurrentHashMap<String, AggMonitorValue> aggHistoryMap,
                        ConcurrentHashMap<String, MonitorValue> discardHistoryMap){
        super(sc);
        if (saveHistoryMap != null) {
            this.saveHistoryMap = saveHistoryMap;
        }
        if (aggHistoryMap != null) {
            this.aggHistoryMap = aggHistoryMap;
        }
        if (discardHistoryMap != null) {
            this.discardHistoryMap = discardHistoryMap;
        }
    }

    public String head() {
        StringBuilder head = new StringBuilder("<div class='row-fluid'><div class='span12'>");
        head.append("<h3 style='vertical-align: bottom; display: inline-block;'>");
        head.append("Monitor");
        head.append("</h3>");

        return head.append("</div></div>").toString();
    }

    /**
     * 明细数据保存到Hbase中的记录
     * 
     * @return
     */
    public String saveHistoryView() {
        StringBuilder html = new StringBuilder("<h4>Save History</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='30.0%' class=''>Unique Id</th>");
        html.append("<th width='30.0%' class=''>Hbase Table</th>");
        html.append("<th width='10.0%' class=''>Type</th>");
        html.append("<th width='15.0%' class=''>Last Save Time</th>");
        html.append("<th width='15.0%' class=''>Num</th>");
        html.append("</tr></thead>");

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);

        html.append("<tbody>");
        for (Entry<String, MonitorValue> item : saveHistoryMap.entrySet()) {
            html.append("<tr>");
            html.append("<td>" + item.getValue().getHbaseTable() + "</td>");
            html.append("<td>" + item.getValue().getSourceName() + "</td>");
            html.append("<td>" + item.getValue().getType() + "</td>");
            html.append("<td>" + format.format(item.getValue().getLastOpTime()) + "</td>");
            html.append("<td>" + item.getValue().getOpCount() + "</td>");
            html.append("</tr>");
        }
        html.append("</tbody>");

        return html.append("<tfoot></tfoot></table>").toString();
    }

    /**
     * 渲染聚合操作保存的数据量(实时)
     * 
     * @return
     */
    public String aggHistoryView() {
        StringBuilder html = new StringBuilder("<h4>Aggregation History</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='60.0%' class=''>Source Name</th>");
        html.append("<th width='10.0%' class=''>Sink</th>");
        html.append("<th width='15.0%' class=''>Last Save Time</th>");
        html.append("<th width='15.0%' class=''>Num</th>");
        html.append("</tr></thead>");

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);

        html.append("<tbody>");

        for (Entry<String, AggMonitorValue> entry : aggHistoryMap.entrySet()) {
            html.append("<tr>");
            html.append("<td >" + entry.getValue().getSourceName() + "</td>");
            html.append("<td >" + entry.getValue().getSink() + "</td>");
            html.append("<td >" + format.format(entry.getValue().getLastOpTime()) + "</td>");
            html.append("<td>" + entry.getValue().getOpCount() + "</td>");
            html.append("</tr>");
        }

        html.append("</tbody>");

        return html.append("<tfoot></tfoot></table>").toString();
    }

    /**
     * 以表格形式渲染被丢弃的数据历史
     * 
     * @return
     */
    public String discardHistoryView() {
        StringBuilder html = new StringBuilder("<h4>Discard History</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='60.0%' class=''>Unique Id</th>");
        html.append("<th width='10.0%' class=''>Type</th>");
        html.append("<th width='15.0%' class=''>Last Request Time</th>");
        html.append("<th width='15.0%' class=''>Batche Num</th>");
        html.append("</tr></thead>");

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);

        html.append("<tbody>");
        for (Entry<String, MonitorValue> item : discardHistoryMap.entrySet()) {
            html.append("<tr>");
            html.append("<td>" + item.getValue().getHbaseTable() + "</td>");
            html.append("<td>" + item.getValue().getType() + "</td>");
            html.append("<td>" + format.format(item.getValue().getLastOpTime()) + "</td>");
            html.append("<td>" + item.getValue().getOpCount() + "</td>");
            html.append("</tr>");
        }
        html.append("</tbody>");

        return html.append("<tfoot></tfoot></table>").toString();
    }

    @Override
    public String bulid(String... appendElements) {
        StringBuilder html = new StringBuilder("<div class='container-fluid'>");
        html.append(head());

        html.append("<span>");
        html.append(saveHistoryView());
        html.append(discardHistoryView());
        html.append(aggHistoryView());
        html.append("</span>");

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }

        return html.append("</div>").toString();
    }

}
