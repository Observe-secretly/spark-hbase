package cn.tsign.ui;

import org.apache.spark.api.java.JavaSparkContext;

public class BodyTemplate extends TemplateAbstract {

    private static final long serialVersionUID = 7972384999289475228L;

    public static int         CONF             = 0;

    public static int         MONITOR          = 1;

    public static int         Operator         = 2;

    public static int         Settings         = 3;

    public static int         MonitorNetwork   = 4;

    public static int         DruidPush        = 5;

    private int               navIndex         = 0;

    public BodyTemplate(JavaSparkContext sc, int navIndex){
        super(sc);
        this.navIndex = navIndex;
    }

    String navbar(int index) {
        StringBuilder navbar = new StringBuilder("<div class='navbar navbar-static-top'>");
        navbar.append("<div class='navbar-inner'>");
        navbar.append("<div class='brand'>");
        navbar.append("<img src='${RES}/static/spark-logo-77x50px-hd.png'>");
        navbar.append("<span class='version'>2.1.1.2.6.1.0-129</span>");
        navbar.append("</div>");

        navbar.append("<p class='navbar-text pull-right'><strong >SparkHbase</strong> Monitor UI</p>");

        navbar.append("<ul class='nav'>");
        navbar.append("<li class='" + (index == 0 ? "active" : "") + "'><a href='/conf'>Config</a></li>");
        navbar.append("<li class='" + (index == 1 ? "active" : "") + "'><a href='/monitor'>Monitor</a></li>");
        navbar.append("<li class='" + (index == 4 ? "active" : "")
                      + "'><a href='/monitorNetwork'>Monitor Network</a></li>");
        navbar.append("<li class='" + (index == 5 ? "active" : "") + "'><a href='/druidPush'>Druid Push</a></li>");
        navbar.append("<li class='" + (index == 2 ? "active" : "") + "'><a href='/operator'>Operator</a></li>");
        navbar.append("<li class='" + (index == 3 ? "active" : "") + "'><a href='/settings'>Settings</a></li>");

        return navbar.append("</ul></div></div>").toString();

    }

    @Override
    public String bulid(String... appendElements) {
        StringBuilder html = new StringBuilder("<body>");
        html.append(navbar(this.navIndex));

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }
        return html.append("</body>").toString();
    }

}
