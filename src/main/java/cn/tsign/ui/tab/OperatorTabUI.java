package cn.tsign.ui.tab;

import org.apache.spark.api.java.JavaSparkContext;

import cn.tsign.ui.TemplateAbstract;

public class OperatorTabUI extends TemplateAbstract {

    private static final long serialVersionUID = -4148702656010745400L;

    public OperatorTabUI(JavaSparkContext sc){
        super(sc);
    }

    public String head() {
        StringBuilder head = new StringBuilder("<div class='row-fluid'><div class='span12'>");
        head.append("<h3 style='vertical-align: bottom; display: inline-block;'>");
        head.append("Operator");
        head.append("</h3></div>");

        return head.append("</div>").toString();
    }

    /**
     * 优雅关闭
     * 
     * @return
     */
    public String elegantKillView() {
        StringBuilder html = new StringBuilder("<h4>Elegant Kill</h4>");
        html.append("<div class='row-fluid' style='margin-top: 0%;'>");
        html.append("<p class='lead' style='font-style:italic;'>");
        html.append("Click \"Kill\" to stop SparkStreaming, waiting to process all data of the current batch. This operation may last for several seconds. ");
        html.append("<button type='button' class='btn btn-danger' onclick=\"javascript:window.location.href='/closeAction'\" >Kill</button></p>");
        html.append("</div>");
        return html.toString();
    }

    public String refreshView() {
        StringBuilder html = new StringBuilder("<h4>Refresh Conf</h4>");
        html.append("<div class='row-fluid' style='margin-top: 0%;'>");
        html.append("<p class='lead' style='font-style:italic;'>");
        html.append("Clicking the Refresh button will Refresh all the configurations. ");
        html.append("<button type='button' class='btn btn-danger' onclick=\"javascript:window.location.href='/refreshConfAction'\" >Refresh</button></p>");
        html.append("</div>");
        return html.toString();
    }

    @Override
    public String bulid(String... appendElements) {
        StringBuilder html = new StringBuilder("<div class='container-fluid'>");
        html.append(head());
        html.append(elegantKillView());
        html.append(refreshView());

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }

        return html.append("</div>").toString();
    }

}
