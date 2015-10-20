# kafka-storm-redis
用flume定制各类数据发送到kafka的topic，storm从kafka topic拉取数据，处理和缓存（redis）的例子。
## flume ->kafka
```shell
flume-ng agent -c /xxx/conf -f /xxx/conf/f1.conf -n agent-1 > /xxx/logfile 2>&1 &
```
### flume配置文件
```xml
#flume-agent配置文件

#基信息配置
agent1.sources = r1
agent1.sinks = k1
agent1.channels = c1

#过滤.tmp文件，防止读写报错
agent1.sources.r1.type = spooldir
agent1.sources.r1.spoolDir = /xxx/spooldir
agent1.sources.r1.ignorePattern = ^(.)*\\.tmp$

#sink配置，这里与kafka对接,输入到kafka中的topic
agent1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
agent1.sinks.k1.topic = test_topic_gn_02
agent1.sinks.k1.batchSize = 100
agent1.sinks.k1.brokerList = hadoop02:9092,hadoop03:9092,hadoop04:9092
agent1.sinks.k1.requiredAcks = 1

#这里采用file作为channel，性能上比 memory下降，但保证了数据的完整性
agent1.channels.c1.type=file
agent1.channels.c1.checkpointDir=/xxx/example_flume_kafka
agent1.channels.c1.dataDirs=/home/xxx/example_flume_kafka

agent1.sources.r1.interceptors = i2
agent1.sources.r1.interceptors.i2.type=org.apache.flume.sink.solr.morphline.UUIDInterceptor$Builder
agent1.sources.r1.interceptors.i2.headerName=key
agent1.sources.r1.interceptors.i2.preserveExisting=false

agent1.sources.r1.channels = c1
agent1.sinks.k1.channel = c1
```
### kafka
```shell
# 启动 kafka
nohup /usr/lib/kafka/kafka_2.11-0.8.2.1/bin/kafka-server-start.sh /usr/lib/kafka/kafka_2.11-0.8.2.1/config/server.properties >/dev/null 2>&1 &
# 创建topic
/usr/lib/kafka/kafka_2.11-0.8.2.1/bin/kafka-topics.sh --create --zookeeper 192.168.1.102:2182,192.168.1.103:2182,192.168.1.104:2182 --replication-factor 3 --partitions 3 --topic test_topic_gn
# 删除topic
/usr/lib/kafka/kafka_2.11-0.8.2.1/bin/kafka-topics.sh --delete --zookeeper 192.168.1.102:2182,192.168.1.103:2182,192.168.1.104:2182 --topic test_topic_gn
# 生产
/usr/lib/kafka/kafka_2.11-0.8.2.1/bin/kafka-console-producer.sh --broker-list 192.168.1.102:9092,192.168.1.103:9092,192.168.1.104:9092 --topic test_topic_gn
# 消费
/usr/lib/kafka/kafka_2.11-0.8.2.1/bin/kafka-console-consumer.sh --zookeeper 192.168.1.102:2182,192.168.1.103:2182,192.168.1.104:2182 --topic test_topic_gn
# 查看 client消费topic offset 
/usr/lib/zookeeper3.4.6/zookeeper-3.4.6/bin/zkCli.sh -server 127.0.0.1:2182
```
##strom-kafka
###kafkaspout
```java
public KafkaSpoutFactory(String topic, String spoutId, String[] zkServers,
			Scheme scheme, String zkRoot, Integer zkPort) {
		StringBuffer zkHosts =null;
		
		if (zkServers != null) {
			zkHosts = new StringBuffer();
			for (String s : zkServers) {
				if (zkHosts.length() > 0)
					zkHosts.append(",");
				zkHosts.append(s);
				zkHosts.append(":");
				zkHosts.append(zkPort);
			}
		} else {
			LOG.error("zkServers is null");
			throw new IllegalArgumentException("zkServers not be null");
		}
		
		this.brokerHosts = new ZkHosts(zkHosts.toString());
		this.spoutConf = new SpoutConfig(this.brokerHosts, topic, zkRoot,spoutId);
		this.spoutConf.scheme = new SchemeAsMultiScheme(scheme);
		// 该配置是指，如果该Topology因故障停止处理，下次正常运行时是否从Spout对应数据源Kafka中的该订阅Topic的起始位置开始读取，如果forceFromStart=true，
		// 则之前处理过的Tuple还要重新处理一遍，否则会从上次处理的位置继续处理，保证Kafka中的Topic数据不被重复处理，是在数据源的位置进行状态记录
		spoutConf.forceFromStart = false;
		this.spoutConf.zkServers = Arrays.asList(zkServers);
		this.spoutConf.zkPort = zkPort;
		this.kafkaSpout = new KafkaSpout(this.spoutConf);
	}
```
