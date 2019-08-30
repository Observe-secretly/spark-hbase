package cn.tsign.entity;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class StoreAbstract implements StoreInterface, Serializable {

    private static final long serialVersionUID = -1687653594320388759L;

    /**
     * 实体对象转成Map
     * 
     * @param obj 实体对象
     * @return
     */
    public static Map<String, Object> object2Map(Object obj) {
        Map<String, Object> map = new HashMap<>();
        if (obj == null) {
            return map;
        }
        Class<? extends Object> clazz = obj.getClass();
        Field[] fields = clazz.getDeclaredFields();
        try {
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.get(obj) instanceof Map) {
                    map.putAll((Map<String, Object>) field.get(obj));
                } else {
                    map.put(field.getName(), field.get(obj));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

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
