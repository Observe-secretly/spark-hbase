package cn.tsign.ui;

import org.apache.spark.api.java.JavaSparkContext;

public class HeadTemplate extends TemplateAbstract {

    private static final long serialVersionUID = -4580022451791186713L;

    private String            title            = "SparkHbase";

    public HeadTemplate(JavaSparkContext sc, String title){
        super(sc);
        this.title = title;
    }

    @Override
    public String bulid(String... appendElements) {
        StringBuffer html = new StringBuffer();
        html.append("<head>");
        // append css
        html.append("<link rel='stylesheet' href='${RES}/static/bootstrap.min.css' type='text/css'>");
        html.append("<link rel='stylesheet' href='${RES}/static/dataTables.bootstrap.css' type='text/css'>");
        html.append("<link rel='stylesheet' href='${RES}/static/jquery.dataTables.1.10.4.min.css' type='text/css'>");
        html.append("<link rel='stylesheet' href='${RES}/static/jsonFormatter.min.css' type='text/css'>");
        html.append("<link rel='stylesheet' href='${RES}/static/spark-dag-viz.css' type='text/css'>");
        html.append("<link rel='stylesheet' href='${RES}/static/timeline-view.css' type='text/css'>");
        html.append("<link rel='stylesheet' href='${RES}/static/vis.min.css' type='text/css'>");
        html.append("<link rel='stylesheet' href='${RES}/static/webui.css' type='text/css'>");

        // append js
        html.append("<script src='${RES}/static/jquery-1.11.1.min.js'></script>");
        html.append("<script src='${RES}/static/jquery.blockUI.min.js'></script>");
        html.append("<script src='${RES}/static/jquery.cookies.2.2.0.min.js'></script>");
        html.append("<script src='${RES}/static/jquery.dataTables.1.10.4.min.js'></script>");
        html.append("<script src='${RES}/static/jquery.mustache.js'></script>");
        html.append("<script src='${RES}/static/bootstrap-tooltip.js'></script>");
        html.append("<script src='${RES}/static/d3.min.js'></script>");
        html.append("<script src='${RES}/static/dagre-d3.min.js'></script>");
        html.append("<script src='${RES}/static/dataTables.bootstrap.min.js'></script>");
        html.append("<script src='${RES}/static/dataTables.rowsGroup.js'></script>");

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }

        html.append("<script src='${RES}/static/graphlib-dot.min.js'></script>");
        html.append("<script src='${RES}/static/initialize-tooltips.js'></script>");
        html.append("<script src='${RES}/static/jsonFormatter.min.js'></script>");
        html.append("<script src='${RES}/static/log-view.js'></script>");
        html.append("<script src='${RES}/static/sorttable.js'></script>");
        html.append("<script src='${RES}/static/spark-dag-viz.js'></script>");
        html.append("<script src='${RES}/static/utils.js'></script>");
        html.append("<script src='${RES}/static/vis.min.js'></script>");
        html.append("<script src='${RES}/static/webui.js'></script>");
        html.append("<title>" + title + "</title>");

        html.append("</head>");

        return html.toString();
    }

}
