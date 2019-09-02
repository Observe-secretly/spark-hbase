# spark-hbase
## 介绍&发展史
随着公司（e签宝）各业务发展，单个业务纵向数据分析已经不能满足运营需要。需要把各业务数据拉通、扁平化到统一的平台进行数据分析。各业务的结构化数据大部分保存在mysql中。关于mysql增量数据同步我们使用canal把mysql binlog分发到Kafka。再由SparkStreaming消费kafka，解析binlog最终保存到hbase；除了结构化数据，还有一些业务本身并不关注，但是又对数据分析很有价值的数据。我们通过提供数据埋点sdk，把数据（这种数据后面统一称为trace数据）以统一的格式发送到kafka（并非直接发送到kafka，你需要更加可靠的方式，才会被业务接受。下面会介绍），再由SparkStreaming负责消费并保存到hbase。我们最初的目标就是这么简单，这也是项目名的由来。后来我们发现，部分数据光接受保存到hbase，不能够被很好的使用。比如埋点的数据，不聚合很难被有效的查询出来并展示。因为它量太大了，汇总起来总是让你抓狂。所以后来我们对Sparkstreaming程序做了加强。让它支持简单的自定义聚合。我们又调研了Apache Druid，因为Ambari和Superset的原因，它很容易被我们使用。所以我们又在SparkStreaming中集成了Druid。这一切都这么美好。有一天忽然有人反馈某些数据库的binlog并没有被我们消费，问题可能出现在cannl也可能出现在SparkStreaming。但谁关心呢，我们居然没有监控。这下好了，我们对SparkStreaming程序进行了第三次升级，在Driver节点上新开了一个web容器，增加了可视化监控，可视化配置，以及方便第三方程序扩展监控的catapi（类似Elasticsearch的catapi）。这就是SparkHbase的由来；

## 开源的目的
项目以分享为主。你可以通过本项目了解到：
- 数据如何被保存到hbase的（新手）；
- 如何通过简单的配置进行聚合（进阶）；
- 数据如何发送到Druid（高阶）；
- 数据流是怎样被我们监控的（高阶）；
- 最后你还能了解到我们是怎样在Driver节点上，通过java代码写出和原生SparkStreaming味道一模一样的UI界面（高阶）；
