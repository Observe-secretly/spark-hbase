package cn.tsign.common.druid.datasources.hdfs;

public enum SegmentGranularityEnum {
                                    year, month, day, hour, minute, second;

    public static SegmentGranularityEnum get(String name) {
        for (SegmentGranularityEnum item : SegmentGranularityEnum.values()) {
            if (item.name().equalsIgnoreCase(name)) {
                return item;
            }
        }

        return day;
    }
}
