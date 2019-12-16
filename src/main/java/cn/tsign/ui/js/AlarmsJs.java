package cn.tsign.ui.js;

public class AlarmsJs extends JsAbstract {

    @Override
    public String bulid() {
        StringBuilder js = new StringBuilder("<script type='text/javascript'> ");
        js.append(getUrlParamFunction());

        js.append("$(document).ready(function(){");

        js.append("$('button[op=true]').on('click',function(){");

        js.append(" var name=$(this).attr('name'); ");

        js.append(" var input=$('input[name='+name+']'); ");
        js.append(" var timeout=input.val(); ");
        js.append(" var conf_type=input.attr('conf_type'); ");
        js.append(" var conf_name=input.attr('conf_name'); ");

        js.append(" window.location.href='/updateAlarmsTimeout?conf_type='+conf_type+'&conf_name='+conf_name+'&timeout='+timeout+'  '");

        js.append("})");

        js.append("});");

        return js.append("</script>").toString();
    }

}
