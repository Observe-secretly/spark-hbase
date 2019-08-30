package cn.tsign.ui.js;

public class AlertJs extends JsAbstract {

    @Override
    public String bulid() {
        StringBuilder js = new StringBuilder("<script type='text/javascript'> ");
        js.append(getUrlParamFunction());

        js.append("$(document).ready(function(){");
        js.append("var errorCode = getUrlParam('errorCode');");
        js.append("var errorMesg='unknown error';");
        js.append("if(errorCode !=null){");
        js.append("");
        js.append("if(errorCode == 1001){errorMesg='PARAM_ERROR';}");
        js.append("else ");
        js.append("if(errorCode == 1002){errorMesg='TRACK_UNIQUEID_IS_NULL';}");
        js.append("else ");
        js.append("if(errorCode == 1003){errorMesg='BINLOG_UNIQUEID_IS_NULL';}");
        js.append("else ");
        js.append("if(errorCode == 1004){errorMesg='CONF_EXIST'}");
        js.append("else ");
        js.append("if(errorCode == 1005){errorMesg='ROWKEY_CONF_IS_NULL';}");
        js.append("");
        js.append("$('.alert [name=content]').text(errorMesg);");
        js.append("$('.alert').show()");
        js.append("}");
        js.append("});");

        return js.append("</script>").toString();
    }

}
