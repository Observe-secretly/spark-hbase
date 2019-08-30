package cn.tsign.entity;

import java.util.List;

import cn.tsign.common.hbase.ColumnFamily;

public interface StoreInterface {

    public List<ColumnFamily> toHbase();

}
