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
# 提交 storm jar
storm jar XJ_atom_events-0.0.1-SNAPSHOT.jar com.nlsoft.bi3.bigdata.storm.topo.Topology /home/stormdev/storm/conf/conf.yaml 
# topology rebalance
storm rebalance test-storm-kafka -n 12 -e kafka-spout=2 msg2fileds-bolt=2 save2redis-bolt=6
# 清除
kill -9 `ps aux|grep redis|grep stormdev|awk '{print $2}'`