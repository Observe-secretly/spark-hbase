package cn.tsign.ui;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;

import cn.tsign.ui.entity.MonitorUIConfig;

public abstract class TemplateAbstract implements TemplateInterface, Serializable {

    private static final long   serialVersionUID = -4900341986896172140L;

    private static final String RES              = "\\$\\{RES\\}";

    private List<String>        elementList      = new ArrayList<String>();

    private MonitorUIConfig     uiConf;

    public TemplateAbstract(JavaSparkContext sc){
        this.uiConf = new MonitorUIConfig(sc);
    }

    /**
     * 追加元素
     * 
     * @param elements
     */
    public void appendElement(String... elements) {
        for (String element : elements) {
            elementList.add(element);
        }
    }

    /**
     * 构建页面
     * 
     * @param elements 页面追加的元素，这些元素将会追加在构建完成的页面后面但在head标签内
     * @return
     */
    public abstract String bulid(String... appendElements);

    @Override
    public String toTemplate() {
        // ${PROXY_URI}/proxy/${SPARK_APP_ID}
        String RES_STR = uiConf.getProxyUri()[0];
        if (uiConf.getSparkMaster().trim().equalsIgnoreCase("yarn")) {
            RES_STR = RES_STR + "/proxy/" + uiConf.getSparkAppId();
        }
        String[] elements = new String[elementList.size()];
        elementList.toArray(elements);
        return bulid(elements).replaceAll(RES, RES_STR);
    }

    public MonitorUIConfig getUiConf() {
        return uiConf;
    }

    public String alertView() {
        StringBuilder html = new StringBuilder("<div class='alert' style='display:none;' >");
        html.append("<button type='button' class='close' data-dismiss='alert'>&times;</button>");
        html.append("<strong>Warning!</strong><label name='content'></label>");
        return html.append("</div>").toString();
    }

}
