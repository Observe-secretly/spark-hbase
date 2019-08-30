package cn.tsign.common.druid.datasources;

import com.google.gson.Gson;

import cn.tsign.common.druid.datasources.hdfs.Config;
import cn.tsign.common.druid.datasources.hdfs.DataSchema;
import cn.tsign.common.druid.datasources.hdfs.MetricsSpec;
import cn.tsign.common.druid.datasources.hdfs.MetricsSpecTypeEnum;
import cn.tsign.common.druid.datasources.hdfs.Parser;
import cn.tsign.common.druid.datasources.hdfs.SegmentGranularityEnum;
import cn.tsign.common.druid.datasources.hdfs.Spec;

public class DruidConfig {

    /**
     * 创建一个把hdfs内的json文件上传到druid的配置文件<br>
     * 
     * @param hdfsParam 请进入HdfsParam类，仔细阅读每个参数的含义
     * @return
     */
    public static String createConfToHdfs(HdfsParam hdfsParam) {
        Config config = new Config();

        // 创建数据源
        Parser parser = new Parser();
        parser.setTimestampSpec("timestamp", null);// 设置时间字段
        parser.setParseSpecFormat("json");// 设置数据格式
        parser.setDimensionsSpec(null, null, hdfsParam.dimensions);

        DataSchema dataSchema = new DataSchema(hdfsParam.dataSourceName, parser);
        dataSchema.setGranularitySpec("uniform", hdfsParam.segmentGranularity, null, hdfsParam.interval);
        dataSchema.putMetricsSpec(hdfsParam.metricsSpecs);

        Spec spec = new Spec(dataSchema);
        spec.setIoConfig("static", hdfsParam.paths);
        spec.setTuningConfig(hdfsParam.partitionsType, hdfsParam.targetPartitionSize);

        config.setSpec(spec);

        return new Gson().toJson(config);
    }

    public static void main(String[] args) {
        HdfsParam hdfsParam = new HdfsParam();
        hdfsParam.setDataSourceName("wiki");
        hdfsParam.setSegmentGranularity(SegmentGranularityEnum.day);
        hdfsParam.setInterval("2015-09-12/2015-09-13");
        hdfsParam.setPartitionsType("hashed");
        hdfsParam.setPaths("quickstart/wikiticker-2015-09-12-sampled.json.gz");
        hdfsParam.setDimensions("channel", "cityName", "comment");
        hdfsParam.setTargetPartitionSize(5000000);
        hdfsParam.setMetricsSpecs(new MetricsSpec(MetricsSpecTypeEnum.count, "count", "count"));

        System.out.println(createConfToHdfs(hdfsParam));
    }

    public static class HdfsParam {

        /**
         * 数据源的名称
         */
        private String                 dataSourceName;
        /**
         * 聚合粒度
         */
        private SegmentGranularityEnum segmentGranularity;

        /**
         * 数据消费时间间隔<br>
         * 例："2015-09-12/2015-09-13"
         */
        private String[]               interval;

        /**
         * 数据文件所在位置
         */
        private String                 paths;

        /**
         * 数据的维度（包含哪些列）
         */
        private String[]               dimensions;

        /**
         * 定义了一些聚合器（aggregators）<br>
         * <table border=1px>
         * <thead >
         * <tr>
         * <th>类型</th>
         * <th>type 可选</th>
         * </tr>
         * </thead> <tbody>
         * <tr>
         * <td>count</td>
         * <td>count</td>
         * </tr>
         * <tr>
         * <td>sum</td>
         * <td>longSum, doubleSum, floatSum</td>
         * </tr>
         * <tr>
         * <td>min/max</td>
         * <td>longMin/longMax, doubleMin/doubleMax, floatMin/floatMax</td>
         * </tr>
         * <tr>
         * <td >first/last</td>
         * <td>longFirst/longLast, doubleFirst/doubleLast, floatFirst/floatLast</td>
         * </tr>
         * <tr>
         * <td>javascript</td>
         * <td >javascript</td>
         * </tr>
         * <tr>
         * <td >cardinality</td>
         * <td>cardinality</td>
         * </tr>
         * <tr>
         * <td >hyperUnique</td>
         * <td>hyperUnique</td>
         * </tr>
         * </tbody>
         * </table>
         */
        private MetricsSpec[]          metricsSpecs;

        /**
         * 分区方式<br>
         * hashed分区首先会选择多个Segment，然后根据每行数据所有列的哈希值对这些Segment进行分区，Segment的数量是输入数据集的基数以及目标分区大小自动确定的。<br>
         * Only-dimension单维度分区。选择作为分区指标的维度列，然后将该维度分隔成连续的不同的分区，每个分区都会包含该维度值在该范围内的所有行。默认情况下使用的维度都是自动指定的。
         */
        private String                 partitionsType;

        /**
         * 包含在分区中的目标行数
         */
        private long                   targetPartitionSize;

        public void setDataSourceName(String dataSourceName) {
            this.dataSourceName = dataSourceName;
        }

        public void setSegmentGranularity(SegmentGranularityEnum segmentGranularity) {
            this.segmentGranularity = segmentGranularity;
        }

        public void setInterval(String... interval) {
            this.interval = interval;
        }

        public void setPartitionsType(String partitionsType) {
            this.partitionsType = partitionsType;
        }

        public void setPaths(String paths) {
            this.paths = paths;
        }

        public void setDimensions(String... dimensions) {
            this.dimensions = dimensions;
        }

        public void setTargetPartitionSize(long targetPartitionSize) {
            this.targetPartitionSize = targetPartitionSize;
        }

        public void setMetricsSpecs(MetricsSpec... metricsSpecs) {
            this.metricsSpecs = metricsSpecs;
        }

    }

}
