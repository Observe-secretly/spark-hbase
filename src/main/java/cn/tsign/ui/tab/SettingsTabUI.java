package cn.tsign.ui.tab;

import org.apache.hadoop.hbase.TableName;
import org.apache.spark.api.java.JavaSparkContext;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.util.StringUtil;
import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.ui.TemplateAbstract;

public class SettingsTabUI extends TemplateAbstract {

    private static final long serialVersionUID = 1833557495386131513L;

    private TableName[]       tableNames;

    private CoreConfig        coreConfig;

    public SettingsTabUI(JavaSparkContext sc, CoreConfig coreConfig, TableName[] tableNames){
        super(sc);
        this.coreConfig = coreConfig;
        this.tableNames = tableNames;
    }

    public String head() {
        StringBuilder head = new StringBuilder("<div class='row-fluid'><div class='span12'>");
        head.append("<h3 style='vertical-align: bottom; display: inline-block;'>");
        head.append("Settings");
        head.append("</h3>");

        return head.append("</div></div>").toString();
    }

    public String addMappingConfView() {
        StringBuilder html = new StringBuilder("<h4>Mapping Config</h4>");

        html.append("<form class='form-inline' method='POST' action='/addMappingConfAction'>");

        html.append("<div class='control-group'>");
        html.append("<label class='control-label'> Unique Id </label>");
        html.append("<div class='controls'>");
        html.append("<input type='text' name='uniqueId' style='height: 3em;width: 100%;'  placeholder='track:[cid]+[event] OR binlog:[database]+[table]'>");
        html.append("</div></div>");

        html.append("<div class='control-group'>");
        html.append("<label class='control-label'> Type </label>");
        html.append("<div class='controls'>");
        html.append("<select class='form-control' name='type' style='height: 3em;width: 100%;'>");
        html.append("<option>track</option>");
        html.append("<option>binlog</option>");
        html.append("</select></div></div>");

        html.append("<div class='control-group'>");
        html.append("<label lass='control-label' for=''> Hbase Table </label>");
        html.append("<div class='controls'>");
        html.append("<select class='form-control' name='tableName' style='height: 3em;width: 100%;'>");
        for (TableName tableName : tableNames) {
            html.append("<option>" + tableName.getNameAsString() + "</option>");
        }
        html.append("</select></div></div>");

        html.append("<button type='submit' class='btn btn-default'>Submit</button>");

        return html.append("</form>").toString();
    }

    public String addRowkeyConfView() {
        StringBuilder html = new StringBuilder("<h4>Rowkey Config</h4>");

        html.append("<form class='form-inline' method='POST' action='/addRowkeyConfAction'>");

        html.append("<div class='control-group'>");
        html.append("<label lass='control-label' for=''> Hbase Table </label>");
        html.append("<div class='controls'>");
        html.append("<select class='form-control' name='tableName' style='height: 3em;width: 100%;'>");
        for (TableName tableName : tableNames) {
            html.append("<option>" + tableName.getNameAsString() + "</option>");
        }
        html.append("</select></div></div>");

        html.append("<div class='control-group'>");
        html.append("<label class='control-label'> Rule </label>");
        html.append("<div class='controls'>");
        html.append("<input type='text' name='rule' style='height: 3em;width: 100%;'  placeholder='Multiple fields are separated by \",\" '>");
        html.append("</div></div>");

        html.append("<button type='submit' class='btn btn-default'>Submit</button>");

        return html.append("</form>").toString();
    }

    public String addAggConfView() {
        String descStype = "style='font-size: xx-small;font-weight: bold;font-family: monospace;'";

        StringBuilder html = new StringBuilder("<h4>Aggregation Config</h4>");

        html.append("<form class='form-inline' method='POST' action='/addAggConfAction'>");

        html.append("<div class='control-group'>");
        html.append("<label lass='control-label' for=''> Hbase Table </label>");
        html.append("<div class='controls'>");
        html.append("<select class='form-control' name='tableName' style='height: 3em;width: 100%;'>");
        for (TableName tableName : tableNames) {
            html.append("<option>" + tableName.getNameAsString() + "</option>");
        }
        html.append("</select></div></div>");

        html.append("<div class='control-group'>");
        html.append("<label lass='control-label' for=''> Sink </label>");
        html.append("<div class='controls'>");
        html.append("<select class='form-control' name='sink' style='height: 3em;width: 100%;'>");
        html.append("<option>hbase</option>");
        html.append("<option>druid</option>");
        html.append("</select></div></div>");

        html.append("<div class='control-group'>");
        html.append("<label lass='control-label' for=''> Segment Granularity </label>");
        html.append("<div class='controls'>");
        html.append("<select class='form-control' name='segmentGranularity' style='height: 3em;width: 100%;'>");
        html.append("<option>year</option>");
        html.append("<option>month</option>");
        html.append("<option>day</option>");
        html.append("<option>hour</option>");
        html.append("<option>minute</option>");
        html.append("</select></div></div>");

        html.append("<div class='control-group'>");
        html.append("<label class='control-label'> Source Name");
        html.append("<i " + descStype
                    + ">( If it represents the Hbase table, make sure that the table already exists and contains a ColumnDescriptor with a number of 0 strings )</i>");
        html.append("</label>");
        html.append("<div class='controls'>");
        html.append("<input type='text' name='sourceName' style='height: 3em;width: 100%;'  placeholder='When Sink uses hbase,It represents the hbase table ;When Sink uses druid,It represents the druid datasource'>");
        html.append("</div></div>");

        html.append("<div class='control-group'>");
        html.append("<label class='control-label'> Group ");
        html.append("<i " + descStype + ">( Function support: time_format(Timestemp Field,\"yyyy-MM-dd HH\") )</i>");
        html.append("</label>");
        html.append("<div class='controls'>");
        html.append("<input type='text' name='group' style='height: 3em;width: 100%;'  placeholder='Multiple fields are separated by \",\" '>");
        html.append("</div></div>");

        html.append("<div class='control-group'>");
        html.append("<label class='control-label'> Agg ");
        html.append("<i " + descStype
                    + ">( Aggregation function support: count() sum(Field) min(Field) max(Field) avg(Field) . Alias Demonstration:count()#con )</i>");
        html.append("</label>");
        html.append("<div class='controls'>");
        html.append("<input type='text' name='agg' style='height: 3em;width: 100%;'  placeholder='Multiple fields are separated by \",\" '>");
        html.append("</div></div>");

        html.append("<button type='submit' class='btn btn-default'>Submit</button>");

        return html.append("</form>").toString();
    }

    public String addNotificationView() {
        String descStype = "style='font-size: xx-small;font-weight: bold;font-family: monospace;'";

        String postUrl = coreConfig.getAlarmNotificationConfig().get(CommonConstant.NOTIFY_CONF_POST_URL);
        String postBody = coreConfig.getAlarmNotificationConfig().get(CommonConstant.NOTIFY_CONF_BODY);

        StringBuilder html = new StringBuilder("<h4>Notification Config</h4>");

        html.append("<form class='form-inline' method='POST' action='/updateAlarmsNotifyConf'>");

        html.append("<div class='control-group'>");
        html.append("<label class='control-label'> Post Url </label>");
        html.append("<div class='controls'>");
        html.append("<input type='text' name='postUrl' style='height: 3em;width: 100%;'  placeholder='' value ='"
                    + (StringUtil.isEmpty(postUrl) ? "" : postUrl) + "' >");
        html.append("</div></div>");

        html.append("<div class='control-group'>");
        html.append("<label class='control-label'> Body ");
        html.append("<i " + descStype
                    + ">( You need to include a ${content} placeholder that represents the alert content to be sent )</i>");
        html.append("</label>");
        html.append("<div class='controls'>");
        html.append("<input type='textarea' name='body' style='height: 3em;width: 100%;'  placeholder='' value = '"
                    + (StringUtil.isEmpty(postBody) ? "" : postBody) + "' >");
        html.append("</div></div>");

        html.append("<button type='submit' class='btn btn-default'>Submit</button>");

        return html.append("</form>").toString();
    }

    @Override
    public String bulid(String... appendElements) {
        StringBuilder html = new StringBuilder("<div class='container-fluid'>");
        html.append(alertView());

        html.append(head());
        html.append(addMappingConfView());
        html.append(addRowkeyConfView());
        html.append(addAggConfView());
        html.append(addNotificationView());

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }

        return html.append("</div>").toString();
    }

}
