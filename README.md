# spark-hbase
## 介绍&发展史
随着公司（**e签宝**）各业务发展，单个业务 **纵向数据分析** 已经不能满足运营需要。需要把各业务数据拉通、扁平化到统一的平台进行数据分析。各业务的**结构化**数据大部分保存在mysql中。关于mysql增量数据同步我们使用 **canal** 把mysql binlog分发到Kafka。再由SparkStreaming消费kafka，解析binlog **最终保存到hbase** ；除了结构化数据，还有一些业务本身并不关注，但是又对数据分析很有价值的数据。我们通过提供数据 **埋点** sdk，把数据（这种数据后面统一称为**track**数据）以统一的格式发送到kafka（并非直接发送到kafka，你需要更加可靠的方式，才会被业务接受。下面会介绍），再由SparkStreaming负责消费并保存到hbase。我们最初的目标就是这么简单，这也是项目名的由来。后来我们发现，部分数据光接受保存到hbase，不能够被很好的使用。比如埋点的数据，不**聚合**很难被有效的查询出来并展示。因为它量太大了，汇总起来总是让你抓狂。所以后来我们对Sparkstreaming程序做了加强。让它支持简单的自定义聚合。我们又调研了**Apache Druid**时序数据库，因为**Ambari**和**Superset**的原因，它很容易被我们使用。所以我们又在SparkStreaming中集成了Druid。这一切都这么美好。有一天忽然有人反馈某些数据库的binlog并没有被我们消费，问题可能出现在cannl也可能出现在SparkStreaming。但谁关心呢，我们居然没有**监控**。这下好了，我们对SparkStreaming程序进行了第三次升级，在Driver节点上新开了一个web容器，增加了**可视化监控**，**可视化配置**，以及方便**第三方程序扩展**监控的catapi（类似Elasticsearch的catapi）。这就是SparkHbase的由来；

## 开源的目的
项目以分享为主。你可以通过本项目了解到：
- 数据如何被保存到hbase的（萌新）；
- 如何通过简单的配置进行聚合（进阶）；
- 数据如何发送到Druid（高阶）；
- 数据流是怎样被我们监控的（高阶）；
- 最后你还能了解到我们是怎样在Driver节点上，通过java代码写出和原生SparkStreaming味道一模一样的UI界面（高阶）；

## 架构图
![](https://github.com/914245697/spark-hbase/blob/master/IMAGE/framework.png)

##### 补充说明：
- 关于Track数据格式，请移驾： https://www.sensorsdata.cn/manual/data_schema.html
- Track数据是通过持久化到本地磁盘，再由安装在业务机器上的日志收集插件（自研）发送到日志服务。日志服务再做转发，转发到数据网关。最终数据网关负责把数据发送到kafka

## 提交历史
代码由公司内网 Tag:2.2 版本迁出到GitHub，所以GitHub上浏览不到代码提交历史。
![](https://github.com/914245697/spark-hbase/blob/master/IMAGE/commitHistory.png)

## 工程配置说明
```
spark-hbase
  |--->cn.tsign.common.config
  |                           --->ConfDev.java        ## 开发环境
  |                           --->ConfTest.java       ## 测试环境
  |                           --->ConfRelease.java    ## 生产环境
  |                           --->Env.java            ## 通过这里指定读取那个配置文件

```
`由于在yarn-cluster上运行通过常规的配置文件的方式，需要使用到hdfs。作者觉得麻烦，所以这里小伙伴可以各自发挥，不一定按照我的配置方式进行`

## 启动命令
```
spark-submit     \
--class cn.tsign.spark.SparkHbase    \
--master yarn-cluster    \
--executor-memory 2G    \
--num-executors 8   \
--executor-cores=2  \
--conf spark.shuffle.memoryFraction=0.6    \
--conf spark.task.maxFailures=8  \
--conf spark.akka.timeout=300  \
--conf spark.network.timeout=300  \
--conf spark.yarn.max.executor.failures=100   \
--queue=spark  \
sparkHbase-2.2.jar
```
`命令中的附加参数，是我司在正式环境运行的参数。萌新可以参考`

## 监控UI
### 1、打开Yarn ResourceManagerUI。
找到名称为`cn.tsign.spark.SparkHbase`的Application。然后打开`Tracking URL`
![](https://github.com/914245697/spark-hbase/blob/master/IMAGE/resourceMagager.png)

### 2、打开ExecutorsPage
找到Executors中ID为`driver`的Host。
![](https://github.com/914245697/spark-hbase/blob/master/IMAGE/ExecutorsPage.png)

### 3、打开监控UI
打开步骤2中找到的Host，然后在浏览器中打开：然后打开： http://DriverHost:1890
![](https://github.com/914245697/spark-hbase/blob/master/IMAGE/Config.png)

##### 监控UI默认端口是1890（我小时候去过的网吧名字叫1890）


## 可视化配置图文说明
##### 由于本文着重介绍SparkStreaming，所以关于canal和架构图中除了SparkStreaming以外的组件不做讲解；
### 1、Binlog&Track数据保存到Hbase。
![](https://github.com/914245697/spark-hbase/blob/master/IMAGE/MappingConfig.png)

|配置项|含义|
|-----|-----|
| MappingConfig.UniqueId | 通过此配置对数据进行归类。其中Binlog使用`database_table`格式，track使用`cid_event`。萌新需要关注的是如何对数据进行归类，而不是为何作者使用这样的规则进行分类 |
| MappingConfig.Type | 数据来源自那里 |
| MappingConfig.HbaseTable | 数据保存在Hbase的那个表中 |
| RowkeyConfig.HbaseTable | Hbase表名 |
| RowkeyConfig。Rule | 通过什么数据生成Rowkey，多个字段使用英文逗号隔开 |




