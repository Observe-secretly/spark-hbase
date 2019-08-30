package cn.tsign.ui.entity;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.api.java.JavaSparkContext;

import cn.tsign.common.constant.CommonConstant;

public class MonitorUIConfig implements Serializable {

    private static final long serialVersionUID = 4407605041421323789L;
    private String            sparkAppId       = "";
    private String            driverHost       = "";
    private int               monitorPort      = 1890;
    private String            sparkMaster      = "";
    private Date              appStartTime     = null;

    private String[]          proxyUriBases;
    private String[]          proxyUri         = new String[] { "http://localhost:4040" };

    public MonitorUIConfig(){
    }

    private String getConf(JavaSparkContext sc, String key) {
        try {
            return sc.getConf().get(key);
        } catch (Exception e) {
            return null;
        }
    }

    public MonitorUIConfig(JavaSparkContext sc){
        this.sparkAppId = getConf(sc, "spark.app.id");
        this.driverHost = getConf(sc, "spark.driver.host");
        this.sparkMaster = getConf(sc, "spark.master");
        try {
            this.appStartTime = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS).parse(getConf(sc,
                                                                                                       CommonConstant.SPARK_CONF_START_TIME));
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        String PROXY_URI_BASES_CONF = getConf(sc,
                                              "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES");

        if (PROXY_URI_BASES_CONF != null && PROXY_URI_BASES_CONF.trim() != "") {
            proxyUriBases = PROXY_URI_BASES_CONF.split(",");
            proxyUri = new String[proxyUriBases.length];
            for (int i = 0; i < proxyUriBases.length; i++) {
                String item = proxyUriBases[i];
                try {
                    URL url = new URL(item);
                    proxyUri[i] = url.getProtocol() + "://" + url.getHost() + ":" + url.getPort();

                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String getSparkAppId() {
        return sparkAppId;
    }

    public String getDriverHost() {
        return driverHost;
    }

    public int getMonitorPort() {
        return monitorPort;
    }

    public String[] getProxyUriBases() {
        return proxyUriBases;
    }

    public String[] getProxyUri() {
        return proxyUri;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public Date getAppStartTime() {
        return appStartTime;
    }

}
