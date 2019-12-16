package cn.tsign.entity;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class StoreAbstract implements StoreInterface, Serializable {

    private static final long serialVersionUID = -1687653594320388759L;

    public static byte[] getValue(Object v) {
        if (v instanceof Long) {
            return Bytes.toBytes((Long) v);
        } else if (v instanceof Integer) {
            return Bytes.toBytes((Integer) v);
        } else if (v instanceof Short) {
            return Bytes.toBytes((Short) v);
        } else if (v instanceof Float) {
            return Bytes.toBytes((Float) v);
        } else if (v instanceof Double) {
            return Bytes.toBytes((Double) v);
        } else if (v instanceof BigDecimal) {
            return Bytes.toBytes(v.toString());
        } else if (v instanceof Boolean) {
            return Bytes.toBytes((Boolean) v);
        } else if (v instanceof String) {
            return Bytes.toBytes(v.toString());
        }
        return Bytes.toBytes(v.toString());
    }

}
