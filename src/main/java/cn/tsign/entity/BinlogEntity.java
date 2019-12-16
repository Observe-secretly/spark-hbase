package cn.tsign.entity;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.hbase.ColumnFamily;
import cn.tsign.common.util.StringUtil;

public class BinlogEntity extends StoreAbstract {

    private static final long         serialVersionUID = 1593029012002233528L;

    private List<Map<String, String>> data;

    private String                    database;

    private String                    es;

    private long                      id;

    private boolean                   isDdl;

    private Map<String, String>       mysqlType;

    private List<Map<String, String>> old;

    private Map<String, String>       pkNames;

    private String                    sql;

    private Map<String, Integer>      sqlType;

    private String                    table;

    private String                    ts;

    private String                    type;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getEs() {
        return es;
    }

    public void setEs(String es) {
        this.es = es;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isDdl() {
        return isDdl;
    }

    public void setDdl(boolean isDdl) {
        this.isDdl = isDdl;
    }

    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public Map<String, String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(Map<String, String> pkNames) {
        this.pkNames = pkNames;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, Integer> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, Integer> sqlType) {
        this.sqlType = sqlType;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Map<String, String>> getData() {
        return data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }

    public List<Map<String, String>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, String>> old) {
        this.old = old;
    }

    @Override
    public List<ColumnFamily> toHbase() {
        // XXX 跳过执行DELETE类型的数据
        if (this.type.equalsIgnoreCase("delete")) {
            return Lists.newArrayList();
        }
        if (data == null || data.size() == 0) {
            return Lists.newArrayList();
        }
        List<ColumnFamily> result = new ArrayList<>();
        for (Map<String, String> item : data) {
            result.add(bundleColumnFamily(item));
        }
        return result;
    }

    public ColumnFamily bundleColumnFamily(Map<String, String> maopData) {
        ColumnFamily columnFamily = new ColumnFamily(CommonConstant.COLUMN_FAMILY_NAME);

        Map<String, String> typeMap = bundleTypeMapping();
        for (Entry<String, String> item : maopData.entrySet()) {
            String fieldType = typeMap.get(item.getKey());
            columnFamily.add(item.getKey(),
                             item.getValue() == null ? Bytes.toBytes("") : getValue(fieldType, item.getValue()));
        }

        // 操作标记
        columnFamily.add("operation_flag", Bytes.toBytes(this.getType()));
        // 添加时间列
        columnFamily.add("utc_timestamp", Bytes.toBytes(this.getTs()));
        // 添加天分区字段
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        columnFamily.add("modifytime_day", Bytes.toBytes(format.format(new Date(Long.parseLong(this.getTs())))));

        return columnFamily;
    }

    /**
     * 把mysql的类型映射成hbase的类型<br>
     * Key:fieldName Value:HbaseType
     * 
     * @param mysqlType
     * @return
     */
    private Map<String, String> bundleTypeMapping() {
        Map<String, String> result = new HashMap<>();
        for (Entry<String, String> item : mysqlType.entrySet()) {
            result.put(item.getKey(), mysqlTypeMappingHbase(item.getValue()));
        }
        return result;
    }

    private String mysqlTypeMappingHbase(String mysqlType) {
        if (StringUtils.isEmpty(mysqlType)) {
            return "string";
        }

        if (mysqlType.toLowerCase().startsWith("char")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("varchar")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("tinytext")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("text")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("mediumtext")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("longtext")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("tinyint")) {
            return INT_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("smallint")) {
            return INT_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("mediumint")) {
            return INT_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("int")) {
            return INT_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("bigint")) {
            return LONG_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("float")) {
            return FLOAT_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("double")) {
            return DOUBLE_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("decimal")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("date")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("datetime")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("timestamp")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("time")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("enum")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("set")) {
            return STRING_TYPE;
        } else if (mysqlType.toLowerCase().startsWith("blob")) {
            return STRING_TYPE;
        }
        return STRING_TYPE;
    }

    public static final String LONG_TYPE       = "long";
    public static final String INT_TYPE        = "int";
    public static final String SHORT_TYPE      = "short";
    public static final String FLOAT_TYPE      = "float";
    public static final String DOUBLE_TYPE     = "double";
    public static final String BIGDECIMAL_TYPE = "bigdecimal";
    public static final String BOOLEAN_TYPE    = "boolean";
    public static final String STRING_TYPE     = "string";

    /**
     * 获取字段类型
     * 
     * @param fieldName
     * @return
     */
    public String getFieldType(String fieldName) {
        Map<String, String> typeMap = bundleTypeMapping();
        String fieldHbaseType = typeMap.get(fieldName);
        return fieldHbaseType;
    }

    public String recover(String fieldName, byte[] data) {
        String fieldHbaseType = getFieldType(fieldName);

        try {
            switch (fieldHbaseType.trim().toLowerCase()) {
                case LONG_TYPE:
                    return String.valueOf(Bytes.toLong(data));
                case INT_TYPE:
                    return String.valueOf(Bytes.toInt(data));
                case SHORT_TYPE:
                    return String.valueOf(Bytes.toShort(data));
                case FLOAT_TYPE:
                    return String.valueOf(Bytes.toFloat(data));
                case DOUBLE_TYPE:
                    return String.valueOf(Bytes.toDouble(data));
                case BIGDECIMAL_TYPE:
                    return String.valueOf(Bytes.toBigDecimal(data));
                case BOOLEAN_TYPE:
                    return String.valueOf(Bytes.toBoolean(data));
                case STRING_TYPE:
                    return Bytes.toString(data);
                default:
                    return Bytes.toString(data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Bytes.toString(data);

    }

    private byte[] getValue(String type, String v) {
        if (StringUtils.isEmpty(type)) {
            return Bytes.toBytes(v);
        }
        if (StringUtil.isEmpty(v)) {
            return Bytes.toBytes("");
        }

        try {
            switch (type.trim().toLowerCase()) {
                case LONG_TYPE:
                    return Bytes.toBytes(Long.parseLong(v));
                case INT_TYPE:
                    return Bytes.toBytes(Integer.parseInt(v));
                case SHORT_TYPE:
                    return Bytes.toBytes(Short.parseShort(v));
                case FLOAT_TYPE:
                    return Bytes.toBytes(Float.parseFloat(v));
                case DOUBLE_TYPE:
                    return Bytes.toBytes(Double.parseDouble(v));
                case BIGDECIMAL_TYPE:
                    return Bytes.toBytes(v);
                case BOOLEAN_TYPE:
                    return Bytes.toBytes(Boolean.parseBoolean(v));
                case STRING_TYPE:
                    return Bytes.toBytes(v);
                default:
                    return Bytes.toBytes(v);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return Bytes.toBytes(v);
    }

}
