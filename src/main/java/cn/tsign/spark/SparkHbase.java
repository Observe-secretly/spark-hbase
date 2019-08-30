package cn.tsign.spark;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.googlecode.aviator.AviatorEvaluator;

import cn.tsign.common.config.ConfigConstant;
import cn.tsign.common.config.ConfigFileInit;
import cn.tsign.common.config.Env;
import cn.tsign.common.constant.CommonConstant;
import cn.tsign.common.constant.FunctionNameConstant;
import cn.tsign.common.constant.SinkEnum;
import cn.tsign.common.druid.datasources.DruidConfig;
import cn.tsign.common.druid.datasources.DruidConfig.HdfsParam;
import cn.tsign.common.druid.datasources.hdfs.MetricsSpec;
import cn.tsign.common.druid.datasources.hdfs.MetricsSpecTypeEnum;
import cn.tsign.common.druid.datasources.hdfs.SegmentGranularityEnum;
import cn.tsign.common.druid.util.DruidTaskInfo;
import cn.tsign.common.druid.util.DruidUtils;
import cn.tsign.common.hbase.ColumnFamily;
import cn.tsign.common.hbase.HbaseUtil;
import cn.tsign.common.hbase.Row;
import cn.tsign.common.util.ConfProperties;
import cn.tsign.common.util.HdfsPropFileUtil;
import cn.tsign.common.util.HdfsUtils;
import cn.tsign.common.util.StringUtil;
import cn.tsign.entity.AggregationFieldEntity;
import cn.tsign.entity.AggregationOperatorEnum;
import cn.tsign.entity.BinlogEntity;
import cn.tsign.entity.StoreAbstract;
import cn.tsign.entity.TrackEntity;
import cn.tsign.jetty.JettyDefaultHandler;
import cn.tsign.spark.accumulator.AggMonitorAccumulator;
import cn.tsign.spark.accumulator.MonitorAccumulator;
import cn.tsign.spark.broadcast.ConfBroadcast;
import cn.tsign.spark.broadcast.FlushDruidTaskBroadcast;
import cn.tsign.spark.broadcast.HbaseClientBroadcast;
import cn.tsign.spark.broadcast.HbaseTableNamesBroadcast;
import cn.tsign.spark.broadcast.HdfsUtilBroadcast;
import cn.tsign.spark.broadcast.entity.CoreConfig;
import cn.tsign.spark.broadcast.entity.CoreConfig.AggConfig;
import cn.tsign.spark.decoder.BinlogDecoder;
import cn.tsign.spark.decoder.TrackDecoder;
import cn.tsign.ui.entity.AggMonitorValue;
import cn.tsign.ui.entity.MonitorValue;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkHbase {

    protected static final AtomicInteger nonce                 = new AtomicInteger(0);
    protected static String              randomKey             = RandomStringUtils.randomAlphanumeric(10);

    // spark的应用名称
    private static String                appName;

    // spark的master地址
    private static String                master;

    // 接收数据的地址和端口
    private static String                zkQuorum;

    // 话题所在的组
    private static String                group;

    // 每个话题的分片数
    private static int                   numThreads;

    private static SparkConf             sparkConf;

    private static JavaSparkContext      sc;

    public static JavaStreamingContext   jssc;

    public static MonitorAccumulator     monitorAccumulator    = new MonitorAccumulator();

    public static AggMonitorAccumulator  aggMonitorAccumulator = new AggMonitorAccumulator();

    private static Map<String, Integer>  collectTopic;

    private static Map<String, Integer>  binlogTopic;

    private static String                logLevel              = "INFO";

    private static void init() throws Exception {
        ConfigFileInit.init();

        System.setProperty("es.set.netty.runtime.available.processors", "false");
        System.out.println("ENV:" + Env.getEnvironment());

        appName = ConfProperties.getStringValue(ConfigConstant.app_name);
        master = ConfProperties.getStringValue(ConfigConstant.master);
        zkQuorum = ConfProperties.getStringValue(ConfigConstant.zk_quorum);
        group = ConfProperties.getStringValue(ConfigConstant.group);
        numThreads = ConfProperties.getIntegerValue(ConfigConstant.num_threads);
        logLevel = ConfProperties.getStringValue(ConfigConstant.log_level);

        // 初始化spark
        sparkConf = new SparkConf().setAppName(appName + "JavaStreamingContext-1.0").setMaster(master);
        // 确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");

        // 开启后spark自动根据系统负载选择最优消费速率
        sparkConf.set("spark.streaming.backpressure.enabled", "true");

        // spark.streaming.backpressure.enabled开启的情况下，限制第一次批处理应该消费的数据，因为程序冷启动 队列里面有大量积压，防止第一次全部读取，造成系统阻塞
        sparkConf.set("spark.streaming.backpressure.initialRate", "20");

        /*
         * 参数说明：如果使用HashShuffleManager，该参数有效。如果设置为true，那么就会开启consolidate机制，会大幅度合并shuffle write的输出文件，对于shuffle read
         * task数量特别多的情况下，这种方法可以极大地减少磁盘IO开销，提升性能。
         * 调优建议：如果的确不需要SortShuffleManager的排序机制，那么除了使用bypass机制，还可以尝试将spark.shffle.manager参数手动指定为hash，使用HashShuffleManager
         * ，同时开启consolidate机制。在实践中尝试过，发现其性能比开启了bypass机制的SortShuffleManager要高出10%~30%
         */
        sparkConf.set("spark.shuffle.consolidateFiles", "true");

        sparkConf.set("spark.driver.allowMultipleContexts", "true");

        sparkConf.set(CommonConstant.SPARK_CONF_START_TIME,
                      new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS).format(new Date()));

        sc = new JavaSparkContext(sparkConf);
        System.out.println("defaultMinPartitions:" + sc.defaultMinPartitions()
                           + ".Default min number of partitions for Hadoop RDDs when not given by user");
        sc.setLogLevel(logLevel);
        jssc = new JavaStreamingContext(sc,
                                        new Duration(ConfProperties.getIntegerValue(ConfigConstant.collector_poll_time)));// 设置轮询时间。用来控制多久从kafka读取数据

        // 格式化topic
        binlogTopic = new HashMap<>();// 把topics（多个空格隔开）以逗号切割并处理成map
        String[] topicsArr = ConfProperties.getStringValue(ConfigConstant.binlog_topic).split(" ");
        for (int i = 0; i < topicsArr.length; i++) {
            binlogTopic.put(topicsArr[i].trim(), numThreads);
        }

        collectTopic = new HashMap<>();// 把topics（多个空格隔开）以逗号切割并处理成map
        String[] collectTopicsArr = ConfProperties.getStringValue(ConfigConstant.collect_topic).split(" ");
        for (int i = 0; i < collectTopicsArr.length; i++) {
            collectTopic.put(collectTopicsArr[i].trim(), numThreads);
        }

    }

    public static void main(String[] args) throws Exception {

        init();

        // 注册累加器，用于监控
        sc.sc().register(monitorAccumulator, "monitor-accumulator");
        sc.sc().register(aggMonitorAccumulator, "monitor-aggaccumulator");

        // 订阅topic并创建
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", zkQuorum);
        kafkaParams.put("group.id", group);
        kafkaParams.put("zookeeper.connection.timeout.ms", "10000");

        // 埋点数据采集
        JavaPairReceiverInputDStream<String, TrackEntity> trackLines = KafkaUtils.createStream(jssc, String.class,
                                                                                               TrackEntity.class,
                                                                                               StringDecoder.class,
                                                                                               TrackDecoder.class,
                                                                                               kafkaParams,
                                                                                               collectTopic,
                                                                                               StorageLevel.MEMORY_ONLY_SER());

        JavaDStream<TrackEntity> trackStream = trackLines.flatMap(t -> Lists.newArrayList(t._2).iterator());

        // binlog数据采集
        JavaPairReceiverInputDStream<String, BinlogEntity> binlogLines = KafkaUtils.createStream(jssc, String.class,
                                                                                                 BinlogEntity.class,
                                                                                                 StringDecoder.class,
                                                                                                 BinlogDecoder.class,
                                                                                                 kafkaParams,
                                                                                                 binlogTopic,
                                                                                                 StorageLevel.MEMORY_ONLY_SER());

        JavaDStream<BinlogEntity> binlogStream = binlogLines.flatMap(t -> Lists.newArrayList(t._2).iterator());

        // 根据分库逻辑判定进行切分
        JavaPairDStream<String, List<TrackEntity>> trackSplitDstream = splitTrack(trackStream);
        JavaPairDStream<String, List<BinlogEntity>> binlogSplitDstream = splitBinlog(binlogStream);

        // 存储到Hbase/druid
        saveTrackToHbase(trackSplitDstream);
        saveBinlogToHbase(binlogSplitDstream);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> jssc.stop(true, true)));

        // 添加jetty容器，此容器仅存在与Driver节点上，通过Driver的host:1890访问。
        // 它提供此程序各项关键指标的监控，并且提供可视化UI提供查看
        Server server = new Server(1890);
        ContextHandler closeContext = new ContextHandler("/");
        closeContext.setHandler(new JettyDefaultHandler(sc, jssc, monitorAccumulator, aggMonitorAccumulator, server));
        server.setHandler(closeContext);
        server.start();

        jssc.start();
        jssc.awaitTermination();

    }

    /**
     * 根据切分Key获取Hbase表名
     * 
     * @param splitKey
     * @param jsc
     * @return
     */
    private static TableName getTableName(String splitKey, HbaseUtil client, CoreConfig conf, String type,
                                          String[] tableNames) {
        String defaultTableName = splitKey;

        String confTableName = null;
        switch (type) {
            case CommonConstant.DATA_TYPE_TRACK:
                confTableName = conf.getTrackTableNameSetting(defaultTableName);
                break;
            case CommonConstant.DATA_TYPE_BINLOG:
                confTableName = conf.getBinlogTableNameSetting(defaultTableName);
                break;
            default:
                break;
        }

        if (confTableName != null) {
            defaultTableName = confTableName;
        }

        for (String item : tableNames) {
            if (item.equalsIgnoreCase(defaultTableName)) {
                return TableName.valueOf(item);
            }
        }

        return null;
    }

    private static <T extends StoreAbstract> String getRowkey(TableName tableName, ColumnFamily columnFamily, T entity,
                                                              CoreConfig conf, String dataType) {
        switch (dataType) {
            case CommonConstant.DATA_TYPE_TRACK:
                return getTrackRowkey(tableName, (TrackEntity) entity, conf);
            case CommonConstant.DATA_TYPE_BINLOG:

                return getBinlogRowkey(tableName, (BinlogEntity) entity, columnFamily, conf);
            default:
                return null;
        }

    }

    private static <T extends StoreAbstract> String getTrackRowkey(TableName tableName, TrackEntity entity,
                                                                   CoreConfig conf) {
        // 获取rowkey生成配置
        String[] fileds = conf.getRowkeySetting(tableName.getNameAsString());
        if (fileds == null) {
            return getDefaultRowKey(Calendar.getInstance());
        }

        StringBuilder rowkey = new StringBuilder();
        // 解析entity，拼装rowkey
        Map<String, Object> filedsMap = entity.object2Map(entity);
        for (String item : fileds) {
            Object v = filedsMap.get(item);
            if (v != null) {
                rowkey.append(v.toString());
            } else if (item.equalsIgnoreCase("uuid()")) {
                rowkey.append(getUuid());
            }
        }

        return rowkey.toString();
    }

    private static <T extends StoreAbstract> String getBinlogRowkey(TableName tableName, BinlogEntity entity,
                                                                    ColumnFamily columnFamily, CoreConfig conf) {

        // 获取rowkey生成配置
        String[] fileds = conf.getRowkeySetting(tableName.getNameAsString());
        if (fileds == null) {
            return getDefaultRowKey(Calendar.getInstance());
        }

        StringBuilder rowkey = new StringBuilder();
        // 解析entity，拼装rowkey
        for (String item : fileds) {
            byte[] v = columnFamily.get(item);
            if (v != null) {
                // byte数组复原，并转换成String
                rowkey.append(binlogFieldRecover(item, v, entity));
            } else if (item.equalsIgnoreCase("uuid()")) {
                rowkey.append(getUuid());
            }
        }

        return rowkey.toString();
    }

    /**
     * 把从binlog.columnFamily中拿到的byte类型数据转换成String
     * 
     * @param fieldName
     * @param data
     * @param binlogEntity
     * @return
     */
    private static String binlogFieldRecover(String fieldName, byte[] data, BinlogEntity binlogEntity) {
        return binlogEntity.recover(fieldName, data);
    }

    public static String getDefaultRowKey(Calendar cal) {
        String rowKey = String.format("%s-%s-%s", cal.getTimeInMillis(), randomKey, nonce.getAndIncrement());
        return rowKey;
    }

    private static <T extends StoreAbstract> String getMsgType(T entity) {
        String msgType = null;
        if (entity instanceof TrackEntity) {
            msgType = CommonConstant.DATA_TYPE_TRACK;
        } else {
            msgType = CommonConstant.DATA_TYPE_BINLOG;
        }
        return msgType;
    }

    /**
     * 方法提供保存binlog/track数据的具体实现
     * 
     * @param <T>
     * @param entityMap
     * @param jsc
     * @throws Exception
     */
    private static <T extends StoreAbstract> void saveAction(Map<String, List<T>> entityMap, JavaSparkContext jsc,
                                                             String msgType) throws Exception {

        Broadcast<HbaseUtil> hbaseUtilBroadcast = HbaseClientBroadcast.getInstance(jsc);
        HbaseUtil hbaseClient = hbaseUtilBroadcast.getValue();

        Broadcast<CoreConfig> confBroadcast = ConfBroadcast.getInstance(jsc, false);
        Broadcast<String[]> tableNamesBroadcast = HbaseTableNamesBroadcast.getInstance(jsc, hbaseClient);

        // 根据真是存在的Hbase表进行分类，后期聚合需要
        Map<String, List<T>> aggregationMap = new HashMap<>();

        for (Entry<String, List<T>> item : entityMap.entrySet()) {
            List<Row> rowList = new ArrayList<>();

            // 从HbaseTableNamesBroadcast中拿到表名，没有获取到则跳过
            TableName tableName = null;
            tableName = getTableName(item.getKey(), hbaseClient, confBroadcast.getValue(), msgType,
                                     tableNamesBroadcast.getValue());
            if (tableName == null) {
                // 输出日志，可以在spark监控日志中看到哪些数据没有对应的Hbase表
                addMonitor(item.getKey(), null, item.getValue().size(), msgType, CommonConstant.OP_TYPE_DISCARD,
                           SinkEnum.hbase);
                continue;
            }

            for (T entity : item.getValue()) {
                for (ColumnFamily columnFamily : entity.toHbase()) {
                    Row row = new Row(getRowkey(tableName, columnFamily, entity, confBroadcast.getValue(), msgType));
                    row.add(columnFamily);
                    rowList.add(row);
                }
            }
            if (rowList.size() > 0) {
                System.out.println("Save " + msgType + ",table is " + tableName + ",first rowkey:"
                                   + rowList.get(0).getRowKey() + ", num->" + rowList.size());
                addMonitor(item.getKey(), tableName.getNameAsString(), rowList.size(), msgType,
                           CommonConstant.OP_TYPE_SAVE, SinkEnum.hbase);

                hbaseClient.batchInsertData(tableName, rowList);
            }

            List<T> aggrV = aggregationMap.get(tableName.getNameAsString());
            if (aggrV == null) {
                aggrV = item.getValue();
            } else {
                aggrV.addAll(item.getValue());
            }
            aggregationMap.put(tableName.getNameAsString(), aggrV);
        }

        aggregationAction(aggregationMap, confBroadcast.getValue().getAggSettings(), jsc);

        // 按天把数据推送到druid.如果有的话
        // XXX
        // 这里有个设计失误，因为程序中包含两条链路，即：binlog和track，他们都通过saveAction方法保存数据，那么假设track和binlog同时在使用时，同一批次pushDataToDriud将会被调用两次。存在并发问题<br>
        // 又由于聚合目前只支持track格式，所以这里通过msgtype来控制只有track数据才会执行pushDataToDriud方法。后面此处实现需要修改
        if (msgType.equalsIgnoreCase(CommonConstant.DATA_TYPE_TRACK)) {
            pushDataToDriud(jsc, confBroadcast.getValue().getAggSettings());
        }
    }

    /**
     * 推送数据到druid
     * 
     * @param jsc
     * @throws IOException
     */
    private static void pushDataToDriud(JavaSparkContext jsc,
                                        Map<String, List<AggConfig>> aggConfMap) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD_HH_MM_SS);
        System.out.println("检查是否需要推送数据文本到Druid " + format.format(new Date(System.currentTimeMillis())));

        Broadcast<HdfsUtils> hdfsUtilsBroadcast = HdfsUtilBroadcast.getInstance(jsc);
        HdfsUtils hdfsUtils = hdfsUtilsBroadcast.getValue();

        // 处理待提交的Druid数据文件
        Broadcast<List<String>> untreated = FlushDruidTaskBroadcast.getInstance(jsc, hdfsUtils);
        if (untreated == null || untreated.getValue().size() == 0) return;

        System.out.println("以下文件将会推送到Druid：" + JSON.toJSONString(untreated.getValue()));

        // 提交untreated到Druid,提交之后释放untreated
        List<String> untreatedFileList = untreated.getValue();
        if (untreatedFileList != null && untreatedFileList.size() > 0) {
            for (String path : untreatedFileList) {
                // 解析path获取文件的sourceName
                String sourceName = DruidUtils.getSourceNameForFile(path);
                String fileName = DruidUtils.getFileName(path);
                // 通过source查找聚合配置
                AggConfig aggconf = findAggConfig(aggConfMap, sourceName, SinkEnum.druid);
                DruidTaskInfo druidTaskInfo = new DruidTaskInfo(path);
                if (aggconf == null) {// 查找不到配置，则修改文件名称
                    druidTaskInfo.setError(true);
                    druidTaskInfo.setErrorMessage("Configuration not found");
                    HdfsPropFileUtil.updateConf(HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_DRUID_TASK), fileName,
                                                druidTaskInfo.toString(), hdfsUtils);
                    continue;
                }

                // 构建druid配置文件
                String confStr = createDruidConf(sourceName, path, aggconf);

                // 通过http提交推送
                String taskId = DruidUtils.sendDataDruid(confStr);

                if (taskId != null) {
                    // 获取任务ID 更改任务状态
                    druidTaskInfo.setTaskId(taskId);
                    druidTaskInfo.setStatus("pending");
                } else {
                    druidTaskInfo.setError(true);
                    druidTaskInfo.setErrorMessage("send error");
                }

                HdfsPropFileUtil.updateConf(HdfsPropFileUtil.getHdfsFile(CommonConstant.CONF_DRUID_TASK), fileName,
                                            druidTaskInfo.toString(), hdfsUtils);

                // 释放
                FlushDruidTaskBroadcast.clean();
            }
        }
    }

    private static String createDruidConf(String sourceName, String path, AggConfig aggconf) throws ParseException {
        Date startDate = DruidUtils.getFileInterval(path);// 文件的后缀描述了数据属于那天

        SimpleDateFormat format = new SimpleDateFormat(CommonConstant.YYYY_MM_DD);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) + 1);

        String endDateStr = format.format(calendar.getTime());

        HdfsParam hdfsParam = new HdfsParam();
        hdfsParam.setDataSourceName(sourceName);
        hdfsParam.setSegmentGranularity(SegmentGranularityEnum.day);
        hdfsParam.setInterval(format.format(startDate) + "/" + endDateStr);
        hdfsParam.setPartitionsType("hashed");
        hdfsParam.setPaths(path);
        hdfsParam.setTargetPartitionSize(5000000);

        List<String> dimensions = new ArrayList<>();
        for (String field : aggconf.getGroupFields()) {
            if (field.startsWith(FunctionNameConstant.TIME_FORMAT)) continue;
            dimensions.add(field);
        }

        String[] dimensionArray = new String[dimensions.size()];
        dimensions.toArray(dimensionArray);
        hdfsParam.setDimensions(dimensionArray);

        List<MetricsSpec> metricsSpecs = new ArrayList<>();
        for (String columnDefinition : aggconf.getAggFields()) {
            MetricsSpec metricsSpec = null;
            AggregationFieldEntity column = analysisColumnDefinition(columnDefinition);
            switch (column.getOperator()) {
                case MIN:
                    metricsSpec = new MetricsSpec(MetricsSpecTypeEnum.doubleMin, column.getAggFieldName(),
                                                  column.getAggFieldName());
                    break;
                case MAX:
                    metricsSpec = new MetricsSpec(MetricsSpecTypeEnum.doubleMax, column.getAggFieldName(),
                                                  column.getAggFieldName());
                    break;
                case SUM:
                    metricsSpec = new MetricsSpec(MetricsSpecTypeEnum.doubleSum, column.getAggFieldName(),
                                                  column.getAggFieldName());
                    break;

                default:
                    metricsSpec = new MetricsSpec(MetricsSpecTypeEnum.doubleSum, column.getAggFieldName(),
                                                  column.getAggFieldName());// 如果是count可能没有field字段
                    break;
            }

            metricsSpecs.add(metricsSpec);
        }

        MetricsSpec[] metricsSpecArray = new MetricsSpec[metricsSpecs.size()];
        metricsSpecs.toArray(metricsSpecArray);
        hdfsParam.setMetricsSpecs(metricsSpecArray);
        return DruidConfig.createConfToHdfs(hdfsParam);
    }

    /**
     * 根据sourceName查找聚合配置
     * 
     * @param aggConf
     * @param sourceName
     * @return
     */
    private static AggConfig findAggConfig(Map<String, List<AggConfig>> aggConf, String sourceName, SinkEnum sink) {

        for (Entry<String, List<AggConfig>> entry : aggConf.entrySet()) {
            for (AggConfig item : entry.getValue()) {
                if (item.getSourceName().equalsIgnoreCase(sourceName) && item.getSink().equals(sink)) {
                    return item;
                }
            }
        }
        return null;
    }

    private static <T extends StoreAbstract> void aggregationAction(Map<String, List<T>> aggregationMap,
                                                                    Map<String, List<AggConfig>> aggConf,
                                                                    JavaSparkContext jsc) throws Exception {
        if (aggregationMap.size() == 0) {
            return;
        }

        // 暂时只支持Track

        // 1、抛弃非Track的数据
        // 2、匹配聚合配置
        // 3、聚合表是否存在判定
        // 4、得到聚合配置结果
        // 5、入库

        for (Entry<String, List<T>> entry : aggregationMap.entrySet()) {
            String msgType = getMsgType(entry.getValue().get(0));
            if (!msgType.equals(CommonConstant.DATA_TYPE_TRACK)) continue;

            List<AggConfig> aggConfs = aggConf.get(entry.getKey());
            if (aggConfs == null || aggConfs.size() == 0) continue;

            for (AggConfig item : aggConfs) {
                // 聚合数据，并且存储
                aggregationUnit(msgType, entry.getValue(), item, jsc);
            }

        }
    }

    private static <T extends StoreAbstract> void aggregationUnit(String msgType, List<T> data, AggConfig aggConfig,
                                                                  JavaSparkContext jsc) throws Exception {
        // 1、根据配置中的group生成rowkey
        // 2、根据agg生成聚合数据。（需要实现聚合函数和简单的运算）
        // 3、数据存储到hbase

        String[] groupFields = aggConfig.getGroupFields();
        String[] aggFields = aggConfig.getAggFields();

        if (msgType.equals(CommonConstant.DATA_TYPE_TRACK)) {
            // 根据group进行数据切分.
            // rowkey:数据集
            Map<String, List<TrackEntity>> splitData = aggregationSplitTrack(groupFields, (List<TrackEntity>) data);
            // 根据拆分好的数据集进行聚合
            // rowkey:聚合数据集。此数据集List每个元素代表一列，根据operator获取聚合值
            Map<String, List<AggregationFieldEntity>> aggResult = aggRrack(splitData, groupFields, aggFields);
            // 存储结果
            saveAgg(aggConfig, aggResult, msgType, jsc);

        } else if (msgType.equals(CommonConstant.DATA_TYPE_BINLOG)) {
            // 预留Binglog聚合处理位置，暂不实现
        }

    }

    /**
     * 存储聚合结果到hbase/druid
     * 
     * @param aggResult
     * @param hbaseClient
     * @throws InterruptedException
     * @throws IOException
     */
    private static void saveAgg(AggConfig aggConfig, Map<String, List<AggregationFieldEntity>> aggResult,
                                String msgType, JavaSparkContext jsc) throws Exception {

        Broadcast<HbaseUtil> hbaseUtilBroadcast = HbaseClientBroadcast.getInstance(jsc);
        HbaseUtil hbaseClient = hbaseUtilBroadcast.getValue();

        Broadcast<HdfsUtils> hdfsUtilsBroadcast = HdfsUtilBroadcast.getInstance(jsc);
        HdfsUtils hdfsUtils = hdfsUtilsBroadcast.getValue();

        SinkEnum sink = aggConfig.getSink();
        if (sink == null) {
            sink = SinkEnum.hbase;
        }

        switch (sink) {
            case hbase:
                // 如果hbase表不存在则跳过
                Broadcast<String[]> tableNamesBroadcast = HbaseTableNamesBroadcast.getInstance(jsc, hbaseClient);
                if (tableIsExist(tableNamesBroadcast.getValue(), aggConfig.getSourceName())) {
                    saveAggToHbase(aggConfig, aggResult, msgType, hbaseClient);
                }
                break;

            case druid:
                saveAggToDruid(aggConfig, aggResult, msgType, hdfsUtils);
                break;
        }
    }

    private static void saveAggToDruid(AggConfig aggConfig, Map<String, List<AggregationFieldEntity>> aggResult,
                                       String msgType, HdfsUtils hdfsUtils) throws Exception {

        StringBuilder content = new StringBuilder();

        long rowNum = 0;

        for (Entry<String, List<AggregationFieldEntity>> entry : aggResult.entrySet()) {
            JSONObject line = new JSONObject();
            Random r = new Random();// 加上一个毫秒值，防止数据重复
            line.put("timestamp", new DateTime((System.currentTimeMillis() / 1000) * 1000 + 28800000 + r.nextInt(1000),
                                               DateTimeZone.UTC).toString());// 加8小时，转换成UTC时间

            for (AggregationFieldEntity column : entry.getValue()) {
                String aggFieldName = column.getAggFieldName();
                if (aggFieldName == null) continue;

                Object value = null;

                if (column.isNotAggField()) {
                    value = column.getNotAggField();
                } else {
                    // 如果是rowkey维度属性字段则不需要聚合计算
                    switch (column.getOperator()) {
                        case MIN:
                            value = column.calculate().getMin();
                            break;
                        case MAX:
                            value = column.calculate().getMax();
                            break;
                        case AVG:
                            value = column.calculate().getAvg();
                            break;
                        case COUNT:
                            value = new BigDecimal(column.calculate().getCount());
                            break;
                        case SUM:
                            value = column.calculate().getSum();
                            break;

                        default:
                            continue;
                    }
                }
                line.put(aggFieldName, value);
            }
            content.append(line.toJSONString() + "\n");
            rowNum++;
        }

        // 按天写入对应hdfs目录下的文件中即可，文件不存在则创建
        String filePath = ConfProperties.getStringValue(ConfigConstant.druid_agg_data_dir) + "/"
                          + DruidUtils.getTodayDruidDataFileName(aggConfig.getSourceName());
        new HdfsUtils().append(filePath, content.toString());
        addAggMonitor(aggConfig.getUuid(), aggConfig.getSourceName(), rowNum, msgType, SinkEnum.druid);
        System.out.println("save to druid:" + aggConfig.getSourceName() + " number:" + rowNum);

    }

    private static void saveAggToHbase(AggConfig aggConfig, Map<String, List<AggregationFieldEntity>> aggResult,
                                       String msgType, HbaseUtil hbaseClient) throws IOException, InterruptedException {
        List<Row> rowList = new ArrayList<>();
        for (Entry<String, List<AggregationFieldEntity>> entry : aggResult.entrySet()) {
            Row row = new Row(entry.getKey() + getUuid());
            ColumnFamily columnFamily = new ColumnFamily(CommonConstant.COLUMN_FAMILY_NAME);
            for (AggregationFieldEntity column : entry.getValue()) {
                String qualifier = column.getAggFieldName();
                if (qualifier == null) continue;

                byte[] value = Bytes.toBytes("");

                // 如果是rowkey维度属性字段则不需要聚合计算
                if (column.isNotAggField()) {
                    value = StoreAbstract.getValue(column.getNotAggField());
                } else {
                    switch (column.getOperator()) {
                        case MIN:
                            value = Bytes.toBytes(column.calculate().getMin().toString());
                            break;
                        case MAX:
                            value = Bytes.toBytes(column.calculate().getMax().toString());
                            break;
                        case AVG:
                            value = Bytes.toBytes(column.calculate().getAvg().toString());
                            break;
                        case COUNT:
                            value = Bytes.toBytes(column.calculate().getCount());
                            break;
                        case SUM:
                            value = Bytes.toBytes(column.calculate().getSum().toString());
                            break;

                        default:
                            continue;
                    }

                }

                columnFamily.add(qualifier, value);
            }
            row.add(columnFamily);
            rowList.add(row);
        }

        hbaseClient.batchInsertData(TableName.valueOf(aggConfig.getSourceName()), rowList);
        addAggMonitor(aggConfig.getUuid(), aggConfig.getSourceName(), rowList.size(), msgType, SinkEnum.hbase);

    }

    /**
     * Track 聚合数据封装
     * 
     * @param splitData
     * @param aggFields
     * @return
     */
    private static Map<String, List<AggregationFieldEntity>> aggRrack(Map<String, List<TrackEntity>> splitData,
                                                                      String[] groupFields, String[] aggFields) {
        Map<String, List<AggregationFieldEntity>> aggResult = new HashMap<>();

        for (Entry<String, List<TrackEntity>> entry : splitData.entrySet()) {
            // 把对象转换成map对象
            List<Map<String, Object>> trackMapList = convertTracksToMap(entry.getValue());

            List<AggregationFieldEntity> columns = new ArrayList<>();
            // 分别计算出每个聚合函数的值
            for (String columnDefinition : aggFields) {
                AggregationFieldEntity column = analysisColumnDefinition(columnDefinition);
                if (column == null) continue;
                for (Map<String, Object> trackMap : trackMapList) {
                    if (StringUtil.isEmpty(column.getField())) {
                        column.putV(new BigDecimal(0));
                    } else {
                        Object fieldValue = getFieldValue(column.getField(), trackMap);
                        if (fieldValue != null) {
                            column.putV(new BigDecimal(fieldValue.toString()));
                        } else {
                            column.putV(new BigDecimal(0));
                        }
                    }
                }
                columns.add(column);
            }

            // 把rowkey包含的非聚合字段都保存到列表中
            for (String field : groupFields) {
                Object value = trackMapList.get(0).get(field);
                if (value != null) {
                    AggregationFieldEntity column = new AggregationFieldEntity(field, value);
                    columns.add(column);
                }

            }

            aggResult.put(entry.getKey(), columns);

        }
        return aggResult;
    }

    /**
     * 获取field的值<br>
     * 如果field是表达式，则通过AviatorEvaluator计算出结果后返回结果值
     * 
     * @param field
     * @param entryMap
     * @return
     */
    private static Object getFieldValue(String field, Map<String, Object> entryMap) {

        try {
            String expression = field.toLowerCase();
            return AviatorEvaluator.execute(expression, entryMap);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    private static List<Map<String, Object>> convertTracksToMap(List<TrackEntity> tracks) {
        List<Map<String, Object>> resultMap = new ArrayList<Map<String, Object>>();
        for (TrackEntity item : tracks) {
            resultMap.add(StoreAbstract.object2Map(item));
        }
        return resultMap;
    }

    /**
     * 把 sum(field)、min(field)等聚合函数解释成AggregationFieldEntity对象
     * 
     * @return
     */
    private static AggregationFieldEntity analysisColumnDefinition(String columnDefinition) {
        FunctionNameConstant.FunContent funContent = null;
        AggregationOperatorEnum operator = null;

        if (columnDefinition.startsWith(FunctionNameConstant.AGG_MAX)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_MAX, columnDefinition);
            operator = AggregationOperatorEnum.MAX;
        } else if (columnDefinition.startsWith(FunctionNameConstant.AGG_MIN)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_MIN, columnDefinition);
            operator = AggregationOperatorEnum.MIN;
        } else if (columnDefinition.startsWith(FunctionNameConstant.AGG_SUM)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_SUM, columnDefinition);
            operator = AggregationOperatorEnum.SUM;
        } else if (columnDefinition.startsWith(FunctionNameConstant.AGG_COUNT)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_COUNT, columnDefinition);
            operator = AggregationOperatorEnum.COUNT;
        } else if (columnDefinition.startsWith(FunctionNameConstant.AGG_AVG)) {
            funContent = FunctionNameConstant.peel(FunctionNameConstant.AGG_AVG, columnDefinition);
            operator = AggregationOperatorEnum.AVG;
        } else {
            // 错误不支持的函数
            return null;
        }

        return new AggregationFieldEntity(funContent.getAlias(), operator, funContent.getContent());

    }

    /**
     * 聚合操作。根据聚合配置拆分Data数据
     * 
     * @param groupFields
     * @param aggFields
     * @param data
     * @return
     */
    private static Map<String, List<TrackEntity>> aggregationSplitTrack(String[] groupFields, List<TrackEntity> data) {
        // 注意：
        // 聚合函数内的所有字段必须是数值类型。group中的时间格式化函数内字段必须是long类型
        // 确认字段类型后，进行聚合操作

        // 1、根据group把数据分类出来
        // 2、根据拆分出来的数据进行聚合
        Map<String, List<TrackEntity>> splitData = new HashMap<>();
        for (TrackEntity item : data) {
            Map<String, Object> fieldMap = item.object2Map(item);
            StringBuffer rowkey = new StringBuffer();
            for (String fieldName : groupFields) {
                if (fieldName.startsWith(FunctionNameConstant.TIME_FORMAT)) {
                    // 1、Rowkey函数之一 time_format 根据传入的字段和格式化方式进行格式化输出
                    String fieldValue = timeFormat(fieldName, fieldMap);
                    if (fieldValue != null) rowkey.append(fieldValue);

                } else {
                    // 3、取字段值
                    Object fieldValue = fieldMap.get(fieldName);
                    if (fieldValue == null) {
                        // 获取不到配置的字段对应的值，此处不打印日志，因为可能量非常大
                    } else {
                        rowkey.append(fieldValue.toString());
                    }
                }
            }

            List<TrackEntity> splitV = splitData.get(rowkey.toString());
            if (splitV == null) {
                splitV = new ArrayList<>();
            }
            splitV.add(item);
            splitData.put(rowkey.toString(), splitV);
        }

        return splitData;

    }

    public static String getUuid() {
        return UUID.randomUUID().toString();
    }

    public static String timeFormat(String fieldName, Map<String, Object> fieldMap) {
        try {
            // 祛除函数体
            FunctionNameConstant.FunContent funContent = FunctionNameConstant.peel(FunctionNameConstant.TIME_FORMAT,
                                                                                   fieldName);

            String[] split = funContent.getContent().split("\\|");
            // 通过判断split[0] 是否是函数，来实现嵌套函数功能。本期不做

            // 必须是long类型
            Object fieldValue = fieldMap.get(split[0]);
            if (!(fieldValue instanceof Long)) return null;

            SimpleDateFormat format = new SimpleDateFormat(split[1]);

            return format.format(new Date((Long) fieldValue));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * 判断表是否存在
     * 
     * @param tableNames
     * @param tableName
     * @return
     */
    private static boolean tableIsExist(String[] tableNames, String tableName) {
        for (String item : tableNames) {
            if (item.equalsIgnoreCase(tableName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 往累加器中明细数据入库监控记录
     * 
     * @param hbaseTable
     * @param opCount
     * @param opType
     */
    private static void addMonitor(String hbaseTable, String realHbaseTable, long opCount, String msgType,
                                   String opType, SinkEnum sink) {
        MonitorValue monitorValue = new MonitorValue();
        monitorValue.setHbaseTable(hbaseTable);
        monitorValue.setSourceName(realHbaseTable);
        monitorValue.setLastOpTime(new Date());
        monitorValue.setOpCount(BigInteger.valueOf(opCount));
        monitorValue.setType(msgType);
        monitorValue.setOpType(opType);
        monitorValue.setSink(sink);

        monitorAccumulator.add(monitorValue);
    }

    /**
     * 往累加器中聚合数据入库监控记录
     * 
     * @param confUuid
     * @param realHbaseTable
     * @param opCount
     * @param msgType
     * @param sink
     */
    private static void addAggMonitor(String confUuid, String realHbaseTable, long opCount, String msgType,
                                      SinkEnum sink) {
        AggMonitorValue monitorValue = new AggMonitorValue();
        monitorValue.setSourceName(realHbaseTable);
        monitorValue.setLastOpTime(new Date());
        monitorValue.setOpCount(BigInteger.valueOf(opCount));
        monitorValue.setOpType(CommonConstant.OP_TYPE_AGG);
        monitorValue.setType(msgType);
        monitorValue.setSink(sink);
        monitorValue.setUuid(confUuid);

        aggMonitorAccumulator.add(monitorValue);
    }

    private static void saveBinlogToHbase(JavaPairDStream<String, List<BinlogEntity>> binlogSplitDstream) {

        binlogSplitDstream.foreachRDD(new VoidFunction<JavaPairRDD<String, List<BinlogEntity>>>() {

            private static final long serialVersionUID = -7919854734281487114L;

            @Override
            public void call(JavaPairRDD<String, List<BinlogEntity>> t) throws Exception {
                // key结构 database+table
                Map<String, List<BinlogEntity>> entityMap = t.collectAsMap();
                if (entityMap.isEmpty()) {
                    return;
                }

                JavaSparkContext jsc = new JavaSparkContext(t.context());
                saveAction(entityMap, jsc, CommonConstant.DATA_TYPE_BINLOG);
            }
        });
    }

    /**
     * 保存Track数据到Hbase
     * 
     * @param trackSplitDstream
     */
    private static void saveTrackToHbase(JavaPairDStream<String, List<TrackEntity>> trackSplitDstream) {
        trackSplitDstream.foreachRDD(new VoidFunction<JavaPairRDD<String, List<TrackEntity>>>() {

            private static final long serialVersionUID = -7919854734281487114L;

            @Override
            public void call(JavaPairRDD<String, List<TrackEntity>> t) throws Exception {
                // key结构 database+table
                Map<String, List<TrackEntity>> entityMap = t.collectAsMap();
                if (entityMap.isEmpty()) {
                    return;
                }

                JavaSparkContext jsc = new JavaSparkContext(t.context());

                // 保存数据
                saveAction(entityMap, jsc, CommonConstant.DATA_TYPE_TRACK);

            }
        });
    }

    /**
     * 根据database-table进行区分
     * 
     * @param binlogStream
     * @return
     */
    private static JavaPairDStream<String, List<BinlogEntity>> splitBinlog(JavaDStream<BinlogEntity> binlogStream) {
        return binlogStream.flatMapToPair(new PairFlatMapFunction<BinlogEntity, String, List<BinlogEntity>>() {

            private static final long serialVersionUID = -8834313634184025921L;

            @Override
            public Iterator<Tuple2<String, List<BinlogEntity>>> call(BinlogEntity t) throws Exception {
                List<Tuple2<String, List<BinlogEntity>>> result = new ArrayList<>();
                if (t == null) {
                    return result.iterator();
                }
                String key = t.getDatabase() + "-" + t.getTable();
                List<BinlogEntity> value = new ArrayList<>();
                value.add(t);
                result.add(new Tuple2<String, List<BinlogEntity>>(key, value));

                return result.iterator();
            }

        }).reduceByKey(new Function2<List<BinlogEntity>, List<BinlogEntity>, List<BinlogEntity>>() {

            private static final long serialVersionUID = -1023262517342394616L;

            @Override
            public List<BinlogEntity> call(List<BinlogEntity> v1, List<BinlogEntity> v2) throws Exception {
                List<BinlogEntity> result = new ArrayList<>();
                result.addAll(v1);
                result.addAll(v2);
                return result;
            }
        });

    }

    /**
     * 根据cid-event进行切分
     * 
     * @param trackStream
     * @return
     */
    private static JavaPairDStream<String, List<TrackEntity>> splitTrack(JavaDStream<TrackEntity> trackStream) {
        return trackStream.flatMapToPair(new PairFlatMapFunction<TrackEntity, String, List<TrackEntity>>() {

            private static final long serialVersionUID = -5132813419729789061L;

            @Override
            public Iterator<Tuple2<String, List<TrackEntity>>> call(TrackEntity t) throws Exception {
                String key = t.getCid() + "-" + t.getEvent();
                List<TrackEntity> value = new ArrayList<>();
                value.add(t);

                List<Tuple2<String, List<TrackEntity>>> result = new ArrayList<>();
                result.add(new Tuple2<String, List<TrackEntity>>(key, value));
                return result.iterator();
            }

        }).reduceByKey(new Function2<List<TrackEntity>, List<TrackEntity>, List<TrackEntity>>() {

            private static final long serialVersionUID = -1023262517342394616L;

            @Override
            public List<TrackEntity> call(List<TrackEntity> v1, List<TrackEntity> v2) throws Exception {
                List<TrackEntity> result = new ArrayList<>();
                result.addAll(v1);
                result.addAll(v2);
                return result;
            }
        });

    }

}
