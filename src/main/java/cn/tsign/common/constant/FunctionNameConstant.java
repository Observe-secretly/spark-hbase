package cn.tsign.common.constant;

public class FunctionNameConstant {

    public static final String TIME_FORMAT = "time_format(";

    public static final String AGG_MIN     = "min(";

    public static final String AGG_MAX     = "max(";

    public static final String AGG_SUM     = "sum(";

    public static final String AGG_COUNT   = "count(";

    public static final String AGG_AVG     = "avg(";

    /**
     * 祛除函数体
     * 
     * @return
     */
    public static FunContent peel(String funType, String fun) {
        FunContent result = new FunContent();

        fun = fun.trim();

        int aliasIndex = fun.lastIndexOf("#");
        if (aliasIndex > 0) {
            result.setAlias(fun.substring(aliasIndex + 1));
        }

        String content = (aliasIndex > 0 ? fun.substring(0, aliasIndex) : fun).replace(funType, "");
        int closeIndex = content.lastIndexOf(")");
        content = content.substring(0, closeIndex);

        result.setContent(content);
        return result;
    }

    /**
     * 函数主体
     * 
     * @author limin Jul 3, 2019 11:35:22 PM
     */
    public static class FunContent {

        /**
         * 函数主体。他通常是一个或者多个字段名称，后期若支持嵌套函数，则也有可能是嵌套函数本身
         */
        private String content;

        /**
         * 最外层聚合函数必须设置别名
         */
        private String alias;

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

    }

}
