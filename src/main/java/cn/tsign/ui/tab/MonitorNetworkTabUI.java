package cn.tsign.ui.tab;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.api.java.JavaSparkContext;

import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.ui.TemplateAbstract;
import cn.tsign.ui.component.MonitorMapComponent;
import cn.tsign.ui.entity.MonitorValue;

public class MonitorNetworkTabUI extends TemplateAbstract {

    private static final long                       serialVersionUID = -4797724271863122951L;

    private ConcurrentHashMap<String, MonitorValue> saveHistoryMap   = new ConcurrentHashMap<>();

    private CoreConfig                              coreConfig;

    public MonitorNetworkTabUI(JavaSparkContext sc, CoreConfig coreConfig,
                               ConcurrentHashMap<String, MonitorValue> saveHistoryMap){
        super(sc);
        this.coreConfig = coreConfig;
        this.saveHistoryMap = saveHistoryMap;

    }

    public String head() {
        StringBuilder head = new StringBuilder("<div class='row-fluid'><div class='span12'>");
        head.append("<h3 style='vertical-align: bottom; display: inline-block;'>");
        head.append("Monitor Network");
        head.append("</h3>");

        return head.append("</div></div>").toString();
    }

    @Override
    public String bulid(String... appendElements) {
        StringBuilder html = new StringBuilder("<div class='container-fluid'>");
        html.append(head());

        html.append("<span>");
        html.append(new MonitorMapComponent(coreConfig, saveHistoryMap).toTemplate());
        html.append("</span>");

        if (appendElements != null && appendElements.length > 0) {
            for (String element : appendElements) {
                html.append(element);
            }
        }

        return html.append("</div>").toString();
    }

}
