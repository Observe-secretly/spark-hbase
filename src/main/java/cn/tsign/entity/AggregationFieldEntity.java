package cn.tsign.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * 预聚合数据体。里面描述了需要聚合的指标字段和维度字段
 * 
 * @author limin Aug 7, 2019 5:39:08 PM
 */
public class AggregationFieldEntity implements Serializable {

    private static final long       serialVersionUID = 1889670293626834617L;

    /*
     * 觉和字段别名
     */
    private String                  aggFieldName;

    /**
     * 操作类型
     */
    private AggregationOperatorEnum operator;

    /*
     * 对应指标字段 <br> 指标字段可能包含计算公式，可通过getFieldValue获取计算后的值
     */
    private String                  field;

    /**
     * 批数据中，所有field字段的集合<br>
     * 若operator等于COUNT,则会往aggValues中插入若干个0
     */
    private List<BigDecimal>        aggValues        = new ArrayList<BigDecimal>();

    /**
     * 若不是聚合字段，这里对应它的具体值
     */
    private Object                  noAggFieldValue;

    /**
     * 是否是维度字段<br>
     * true：指标<br>
     * false：维度
     */
    private boolean                 isNotAggField    = false;

    public AggregationFieldEntity(String field, Object value){
        this.isNotAggField = true;
        this.field = field;
        this.aggFieldName = field;
        this.noAggFieldValue = value;
    }

    public AggregationFieldEntity(String aggFieldName, AggregationOperatorEnum operator, String field){
        this.isNotAggField = false;
        this.aggFieldName = aggFieldName;
        this.operator = operator;
        this.field = field;
    }

    public void putV(BigDecimal value) {
        aggValues.add(value);
    }

    public String getAggFieldName() {
        return aggFieldName;
    }

    public AggregationOperatorEnum getOperator() {
        return operator;
    }

    public String getField() {
        return field;
    }

    public Object getNotAggField() {
        if (!isNotAggField) {
            return null;
        }
        return noAggFieldValue;
    }

    public boolean isNotAggField() {
        return isNotAggField;
    }

    public Result calculate() {
        if (isNotAggField) {
            return null;
        }
        long count = aggValues.size();
        BigDecimal sum = new BigDecimal(0);

        // 最大最小值默认是aggValues第一个值，否则aggValues.length=1、min默认等于0的时候无法得到最小值
        BigDecimal min = new BigDecimal(aggValues.get(0).doubleValue());
        BigDecimal max = new BigDecimal(aggValues.get(0).doubleValue());

        BigDecimal avg = new BigDecimal(0);

        for (BigDecimal item : aggValues) {
            sum = sum.add(item);
            if (min.compareTo(item) == 1) {// min>item
                min = item;
            }
            if (max.compareTo(item) == -1) {// max<item
                max = item;
            }
        }

        if (sum.intValue() > 0 && count > 0) {
            avg = sum.divide(new BigDecimal(count), 2, RoundingMode.HALF_UP);// 四舍五入
        }

        return new Result(count, sum, min, max, avg);
    }

    public class Result {

        private long       count;

        private BigDecimal sum;

        private BigDecimal min;

        private BigDecimal max;

        private BigDecimal avg;

        private Result(long count, BigDecimal sum, BigDecimal min, BigDecimal max, BigDecimal avg){
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
            this.avg = avg;
        }

        public long getCount() {
            return count;
        }

        public BigDecimal getSum() {
            return sum;
        }

        public BigDecimal getMin() {
            return min;
        }

        public BigDecimal getMax() {
            return max;
        }

        public BigDecimal getAvg() {
            return avg;
        }

    }

}
