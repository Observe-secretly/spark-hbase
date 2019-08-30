package cn.tsign.ui.component;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.ui.entity.MonitorValue;
import cn.tsign.ui.js.MonitorMapJs;
import cn.tsign.ui.js.MonitorMapJs.Edge;
import cn.tsign.ui.js.MonitorMapJs.Node;

public class MonitorMapComponent extends ComponentAbstract {

    private CoreConfig                              coreConfig;

    /**
     * track/binlog save信息监控
     */
    private ConcurrentHashMap<String, MonitorValue> saveHistoryMap;

    private List<Node>                              nodes       = new ArrayList<>();
    private List<Edge>                              edges       = new ArrayList<>();

    private static final Node                       ROOT_NODE   = new MonitorMapJs.Node(0, 1, "Hbase", 0);

    private static final Node                       TRACK_NODE  = new MonitorMapJs.Node(1, 1, "Track", 1);

    private static final Node                       BINLOG_NODE = new MonitorMapJs.Node(2, 1, "Binlog", 2);

    public MonitorMapComponent(CoreConfig coreConfig, ConcurrentHashMap<String, MonitorValue> saveHistoryMap){
        this.coreConfig = coreConfig;
        this.saveHistoryMap = saveHistoryMap;

        ROOT_NODE.setShape("database");

        this.nodes.add(ROOT_NODE);
        this.nodes.add(TRACK_NODE);
        this.nodes.add(BINLOG_NODE);

        this.edges.add(new Edge(TRACK_NODE.getId(), ROOT_NODE.getId(), ""));
        this.edges.add(new Edge(BINLOG_NODE.getId(), ROOT_NODE.getId(), ""));

        draw();

    }

    /**
     * 将监控数据转换成 realHbaseTable:opcount 结构
     * 
     * @return
     */
    private Map<String, BigInteger> transform() {
        // 把saveHistoryMap转换成 realHbaseTable:save number 格式
        Map<String, BigInteger> result = new HashMap<>();
        for (Entry<String, MonitorValue> entry : saveHistoryMap.entrySet()) {
            String realHbaseTable = entry.getValue().getSourceName();
            BigInteger num = entry.getValue().getOpCount();

            BigInteger sum = result.get(realHbaseTable);
            if (sum == null) {
                sum = num;
            } else {
                sum.add(num);
            }
            result.put(realHbaseTable, sum);
        }
        return result;
    }

    private void draw() {

        Map<String, BigInteger> opcountMap = transform();

        HashSet<String> trackRealTables = new HashSet<>();
        HashSet<String> binlogRealTables = new HashSet<>();

        // 循环出来所有的track表名称，并去重
        for (Entry<String, String> item : coreConfig.getTrackTableNameSettings().entrySet()) {
            trackRealTables.add(item.getValue());
        }

        for (Entry<String, String> item : coreConfig.getBinlogTableNameSettings().entrySet()) {
            binlogRealTables.add(item.getValue());
        }

        int i = 3;
        for (String item : trackRealTables) {
            BigInteger num = opcountMap.get(item);
            Node node = new MonitorMapJs.Node(i, (num == null ? 0 : num.longValue()), item, TRACK_NODE.getGroup());
            this.nodes.add(node);
            this.edges.add(new Edge(node.getId(), TRACK_NODE.getId(), (num == null ? "0" : num.toString())));
            i++;
        }
        for (String item : binlogRealTables) {
            BigInteger num = opcountMap.get(item);
            Node node = new MonitorMapJs.Node(i, (num == null ? 0 : num.longValue()), item, BINLOG_NODE.getGroup());
            this.nodes.add(node);
            this.edges.add(new Edge(node.getId(), BINLOG_NODE.getId(), (num == null ? "0" : num.toString())));
            i++;
        }

    }

    @Override
    public String bulid() {
        String elementId = "monitorMapNetwork";
        StringBuilder html = new StringBuilder("<h4>Monitor Network</h4>");
        html.append("<div id='config'></div>");
        html.append("<div id='" + elementId + "' class='row' style='width: 100%;height: 70%;'></div>");
        html.append(new MonitorMapJs(elementId, nodes, edges));

        return html.toString();
    }

}
