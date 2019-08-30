package cn.tsign.ui.js;

public class BlockJs extends JsAbstract {

    private String pageName;

    public BlockJs(String pageName){
        this.pageName = pageName;
    }

    @Override
    public String bulid() {
        StringBuilder js = new StringBuilder("<script type='text/javascript'> ");
        js.append("$(document).ajaxStop($.unblockUI);");
        js.append("$(document).ajaxStart(function(){");
        js.append("$.blockUI({ message: '<h3>Loading " + this.pageName + " page...</h3>'});");
        js.append("});");

        return js.append("</script>").toString();
    }

}
