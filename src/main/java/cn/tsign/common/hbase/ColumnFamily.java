package cn.tsign.common.hbase;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import cn.tsign.common.util.StringUtil;

public class ColumnFamily implements Serializable {

    private static final long             serialVersionUID = 3184582712092698357L;

    private LinkedHashMap<String, byte[]> columns          = new LinkedHashMap<>();

    private String                        familyName;

    private int                           cursor           = 0;

    private QualifierColumn               qualifierColumn  = null;

    /**
     * 设置列族名称
     * 
     * @param familyName
     */
    public ColumnFamily(String familyName){
        this.familyName = familyName;
    }

    /**
     * 获取没有设置qualifier的列值
     * 
     * @return
     */
    public byte[] get() {
        return columns.get(null);
    }

    /**
     * 根据qualifier获取列值
     * 
     * @param qualifier
     * @return
     */
    public byte[] get(String qualifier) {
        return columns.get(qualifier);
    }

    /**
     * 添加一个值
     * 
     * @param qualifier 列修饰符(可以理解成二级列名)
     * @param value 值
     */
    public void add(String qualifier, byte[] value) {
        if (StringUtil.isEmpty(qualifier)) {
            qualifier = null;
        }
        this.columns.put(qualifier, value);
    }

    /**
     * 取值游标下移<br>
     * 此方法配合getQualifierColumn()使用。伪代码如下：<br>
     * ......<br>
     * while(next()!=-1){<br>
     * QualifierColumn qualifierColumn = getQualifierColumn();<br>
     * ...<br>
     * }<br>
     * ..<br>
     * 
     * @return 返回-1时代表已经取尽。返回1时代表取到了当前游标所在的值
     */
    public int hasNext() {
        Iterator<Entry<String, byte[]>> iterator = columns.entrySet().iterator();

        int index = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, byte[]> entry = iterator.next();
            if (index == cursor) {
                qualifierColumn = new QualifierColumn(entry.getKey() == null ? null : entry.getKey().getBytes(),
                                                      entry.getValue());
                cursor++;
                return 1;
            }
            index++;
        }
        qualifierColumn = null;
        return -1;
    }

    /**
     * 获取当前游标下的值<br>
     * 配合next()方法使用，首次取值为null
     * 
     * @return
     */
    public Map.Entry<String, QualifierColumn> next() {

        return new Entry<String, QualifierColumn>() {

            @Override
            public QualifierColumn setValue(QualifierColumn value) {
                return null;
            }

            @Override
            public QualifierColumn getValue() {
                return qualifierColumn;
            }

            @Override
            public String getKey() {
                return familyName;
            }
        };
    }

    public Map<String, byte[]> getColumns() {
        return columns;
    }

    public String getFamilyName() {
        return familyName;
    }

    @Override
    public String toString() {
        StringBuilder rowString = new StringBuilder();
        Iterator<Entry<String, byte[]>> iterator = columns.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, byte[]> entry = iterator.next();
            rowString.append("{" + (StringUtil.isEmpty(entry.getKey()) ? "NULL" : entry.getKey()) + ":"
                             + (entry.getValue() == null ? "NULL" : new String(entry.getValue())) + "}");
        }
        return rowString.toString();
    }

}

class QualifierColumn implements Serializable {

    private static final long serialVersionUID = -8445293164179331310L;
    private byte[]            qualifier;
    private byte[]            v;

    public QualifierColumn(byte[] qualifier, byte[] value){
        this.qualifier = qualifier;
        this.v = value;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public byte[] getV() {
        return v;
    }

}
