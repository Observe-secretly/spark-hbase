package cn.tsign.common.hbase;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

public class Row implements Serializable {

    private static final long                   serialVersionUID = -2009022248384887478L;

    private LinkedHashMap<String, ColumnFamily> columnFamilys    = new LinkedHashMap<String, ColumnFamily>();

    private String                              rowKey;

    public Row(){
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String reverseDate = new StringBuffer(format.format(new Date())).reverse().toString();

        this.rowKey = reverseDate + UUID.randomUUID().toString();
    }

    public Row(String rowKey){
        this.rowKey = rowKey;
    }

    public void add(ColumnFamily columnFamily) {
        String columnFamilyName = columnFamily.getFamilyName();
        columnFamilys.put(columnFamilyName, columnFamily);
    }

    public void add(String columnFamilyName, String qualifier, byte[] value) {
        ColumnFamily columnFamily = columnFamilys.get(columnFamilyName);
        if (columnFamily == null) {
            columnFamily = new ColumnFamily(columnFamilyName);
            columnFamily.add(qualifier, value);
            columnFamilys.put(columnFamilyName, columnFamily);
        } else {
            columnFamily.add(qualifier, value);
            columnFamilys.put(columnFamilyName, columnFamily);
        }
    }

    public String getRowKey() {
        return rowKey;
    }

    public ColumnFamily getColumnFamily(String columnFamilyName) {
        return columnFamilys.get(columnFamilyName);
    }

    public LinkedHashMap<String, ColumnFamily> getColumnFamilys() {
        return columnFamilys;
    }

    @Override
    public String toString() {
        StringBuilder rowString = new StringBuilder("RowKey:" + rowKey + "\n");
        Iterator<Entry<String, ColumnFamily>> iterator = columnFamilys.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ColumnFamily> entry = iterator.next();
            rowString.append("ColumnFamilyName:" + entry.getKey() + ">" + entry.getValue().toString() + "\n");
        }
        rowString.append("<======================================>");
        return rowString.toString();
    }

}
