package cn.tsign.common.druid.datasources.hdfs;

public enum MetricsSpecTypeEnum {
                                 // first/last
                                 longFirst, longLast, doubleFirst, doubleLast, floatFirst, floatLast,
                                 // Min/Max
                                 longMin, longMax, doubleMin, doubleMax, floatMin, floatMax,
                                 // Sum
                                 longSum, doubleSum, floatSum,
                                 // Count
                                 count;
}
