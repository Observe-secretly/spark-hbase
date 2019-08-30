package cn.tsign.ui.js;

import java.util.List;

import com.alibaba.fastjson.JSON;

public class MonitorMapJs extends JsAbstract {

    private String     elementId;

    /**
     * 中心节点
     */
    private List<Node> nodes;

    /**
     * 边缘节点
     */
    private List<Edge> edges;

    public MonitorMapJs(String elementId, List<Node> nodes, List<Edge> edges){
        this.elementId = elementId;
        this.nodes = nodes;
        this.edges = edges;
    }

    @Override
    public String bulid() {
        StringBuilder js = new StringBuilder("<script type='text/javascript'> ");

        js.append("var nodes = " + JSON.toJSONString(nodes) + ";");
        js.append("var edges = " + JSON.toJSONString(edges) + ";");

        js.append("var container = document.getElementById('" + elementId + "');");
        js.append("var data = {nodes: nodes,edges: edges};");
        js.append("var options = {nodes: {shape: 'dot',size: 15,font: {size: 11,color: ''},borderWidth: 1},edges: {width: 0},};");
        js.append("network = new vis.Network(container, data, options);");

        return js.append("</script>").toString();
    }

    public static class Node {

        private int    id;

        private String label;

        private long   value = 1;

        private String shape;

        private int    group;

        public Node(int id, long value, String label, int group){
            this.id = id;
            this.value = value;
            this.label = label;
            this.group = group;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getShape() {
            return shape;
        }

        public void setShape(String shape) {
            this.shape = shape;
        }

        public int getGroup() {
            return group;
        }

        public void setGroup(int group) {
            this.group = group;
        }

        public long getValue() {
            return value;

        }

        public void setValue(long value) {
            // 设置10个大小
            this.value = value;
        }

    }

    public static class Edge {

        /**
         * 边缘节点的ID
         */
        private int    from;

        /**
         * 指向中心节点的ID
         */
        private int    to;

        private String title;

        public Edge(int from, int to, String title){
            this.from = from;
            this.to = to;
            this.title = title;
        }

        public int getFrom() {
            return from;
        }

        public void setFrom(int from) {
            this.from = from;
        }

        public int getTo() {
            return to;
        }

        public void setTo(int to) {
            this.to = to;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

    }

}
