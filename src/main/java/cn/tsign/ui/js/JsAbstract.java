package cn.tsign.ui.js;

public abstract class JsAbstract {

    public abstract String bulid();

    String getUrlParamFunction() {
        StringBuilder function = new StringBuilder("function getUrlParam(name) {");
        function.append("var reg = new RegExp(\"(^|&)\" + name + \"=([^&]*)(&|$)\");");
        function.append("var r = window.location.search.substr(1).match(reg);");
        function.append("if (r != null) return unescape(r[2]); return null;");
        return function.append("}").toString();
    }

    @Override
    public String toString() {
        return bulid();
    }

}
