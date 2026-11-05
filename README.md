项目名称：基于搜狗日志的大数据实时与离线分析可视化系统
项目角色： 独立开发 / 核心开发
技术栈： Hadoop (HDFS HA/YARN)、ZooKeeper、Kafka、Flume、Spark、Flink、MySQL、Java Web、ECharts
项目描述： 该项目模拟海量搜狗用户搜索日志，完成从数据采集、消息缓冲、双引擎计算（实时+离线）到数据存储及 Web 可视化大屏展现的全栈大数据处理闭环。
核心职责与成果：

集群搭建与高可用配置：独立搭建 3 节点 Hadoop 集群，修复并实现了 HDFS HA 与 YARN HA 的自动选主（ZKFC）机制；配置并启动了 3 节点 Kafka 集群，解决了 broker.id 冲突与 listeners 网络绑定问题。

数据采集与接入：使用 Flume（Taildir Source）实时监控本地日志，通过 Avro Sink 将数据双机并联分发至聚合端，最终精准写入 Kafka 的 test_ok Topic 进行削峰解耦。

实时与离线计算引擎开发：

使用 Spark Structured Streaming 与 Flink DataStream 双引擎开发实时业务，完成脏数据过滤、字段严格切分与词频统计，对比了微批与原生流处理的性能差异。

使用 Flink DataSet 开发离线批处理任务，完成历史全量数据的清洗与聚合。

复杂数据清洗与入库：针对原始日志缺失字段导致的越界异常，重构了切分逻辑；针对 Windows 文本自带的隐藏 BOM 头引发的 MySQL 报错，通过字符替换技术彻底解决；自定义 JDBC Sink 实现 ON DUPLICATE KEY UPDATE，保障高频数据入库不冲突。

可视化大屏开发：基于 Java Servlet 构建后端 JSON 接口，结合 ECharts 与 AJAX，开发了暗黑科技风的实时数据可视化大屏，实现了从底层数据流转到前端业务展现的完整闭环。
