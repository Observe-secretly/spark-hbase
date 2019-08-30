package cn.tsign.ui.tab;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.druid.util.DruidTaskInfo;
import cn.tsign.common.util.HdfsPropFileUtil;
import cn.tsign.common.util.HdfsUtils;
import cn.tsign.common.util.StringUtil;
import cn.tsign.ui.TemplateAbstract;

public class DruidPushTabUI extends TemplateAbstract {

    private static final long serialVersionUID = 1519069047202918592L;

    private HdfsUtils         hdfsUtils;

    public DruidPushTabUI(JavaSparkContext sc, HdfsUtils hdfsUtils){
        super(sc);
        this.hdfsUtils = hdfsUtils;
    }

    public String head() {
        StringBuilder head = new StringBuilder("<div class='row-fluid'><div class='span12'>");
        head.append("<h3 style='vertical-align: bottom; display: inline-block;'>");
        head.append("Druid Push");
        head.append("</h3>");

        return head.append("</div></div>").toString();
    }

    public String druidPushTaskView() {
        StringBuilder html = new StringBuilder("<h4>Druid Push</h4>");
        html.append("<table class='table table-bordered table-condensed table-striped sortable'>");
        html.append("<thead><tr>");
        html.append("<th width='40.0%' class=''>Path</th>");
        html.append("<th width='40.0%' class=''>TaskId</th>");
        html.append("<th width='10.0%' class=''>Status</th>");
        html.append("<th width='10.0%' class=''>Message</th>");
        html.append("</tr></thead>");

        Properties druidTaskInfoConfig = new Properties();
        try {
            druidTaskInfoConfig.load(hdfsUtils.readHDFSFileToStream(HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_DRUID_TASK)));
            html.append("<tbody>");

            for (Entry<Object, Object> item : druidTaskInfoConfig.entrySet()) {

                DruidTaskInfo druidTaskInfo = new Gson().fromJson(item.getValue().toString(), DruidTaskInfo.class);

                html.append("<tr>");
                html.append("<td>" + druidTaskInfo.getPath() + "</td>");
                html.append("<td>" + druidTaskInfo.getTaskId() + "</td>");
                html.append("<td>" + formatStatus(druidTaskInfo.getStatus()) + "</td>");
                html.append("<td>"
                            + (StringUtil.isEmpty(druidTaskInfo.getErrorMessage()) ? "Nothing" : druidTaskInfo.getErrorMessage())
                            + "</td>");
                html.append("</tr>");
            }

            html.append("</tbody>");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return html.append("<tfoot></tfoot></table>").toString();
    }

    public String formatStatus(String status) {
        status = status.toUpperCase().trim();
        switch (status) {
            case "SUCCESS":
                return "<font color=green >" + status + "</font>";
            case "FAILED":

                return "<font color=red >" + status + "</font>";

            default:
                return status;
        }
    }

    @Override
    public String bulid(String... appendElements) {
        StringBuilder html = new StringBuilder("<div class='container-fluid'>");
        html.append(head());

        html.append("<span>");
        html.append(druidPushTaskView());
        html.append("</span>");

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }

        return html.append("</div>").toString();
    }

}
