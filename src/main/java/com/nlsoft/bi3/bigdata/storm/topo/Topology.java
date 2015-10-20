/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月27日 下午2:46:42
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.storm.topo;

import java.io.IOException;

import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import com.nlsoft.bi3.bigdata.common.config.ConfigLoader;
import com.nlsoft.bi3.bigdata.common.config.ConfigUtil;
import com.nlsoft.bi3.bigdata.common.config.Default;
import com.nlsoft.bi3.bigdata.common.db.redis.mapper.RedisDataDescription;
import com.nlsoft.bi3.bigdata.common.db.redis.mapper.RedisStoreMapper;
import com.nlsoft.bi3.bigdata.common.db.redis.mapper.RedisDataDescription.RedisDataType;
import com.nlsoft.bi3.bigdata.storm.bolt.Hmset2RedisBolt;
import com.nlsoft.bi3.bigdata.storm.bolt.Msg2KVBolt;
import com.nlsoft.bi3.bigdata.storm.bolt.Msg2MapBolt;
import com.nlsoft.bi3.bigdata.storm.bolt.MultiRedisStoreBolt;
import com.nlsoft.bi3.bigdata.storm.bolt.NewHmset2RedisBolt;
import com.nlsoft.bi3.bigdata.storm.spout.KafkaSpoutFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	public static void main(String[] args) throws IOException, AlreadyAliveException, InvalidTopologyException {
		// 加载配置文件
		Config conf = new ConfigLoader().loadStormConfig(args.length > 0 ? args[0]
						: Default.STORM_CONF_PATH);
		// 构建 topo
		TopologyBuilder builder = new TopologyBuilder();
		buildTopo(builder, conf);
		conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
		// 提交topo
		StormSubmitter.submitTopologyWithProgressBar(conf.get("topology.name")
				.toString(), conf, builder.createTopology());
	}
	
	public static void buildTopo(TopologyBuilder builder,Config conf){

				KafkaSpout kafkaSpout = new KafkaSpoutFactory(conf.get("gn.kafka.spout.topic.name").toString(),
						conf.get("gn.kafka.spout.topic.client.id").toString(),
						ConfigUtil.getStrArrayVal(conf, "storm.zookeeper.servers.ip"),
						new StringScheme(),
						conf.get("kafka.topic.zookeeper.root").toString(),
						Integer.parseInt(conf.get("storm.zookeeper.port").toString()))
				        .getKafkaSpout();
				//0 spout-0-0
				builder.setSpout("spout-00-gn-kafka", kafkaSpout,
						Integer.parseInt(conf.get("gn.kafka.spout.parallelism.hint").toString()));
				//1 bolt-00-10
				builder.setBolt("bolt-00-10-msg2hashMap",new Msg2MapBolt(conf.get("gn.data.schema.fileds").toString(),conf.get("gn.data.split.str").toString()),
						Integer.parseInt(conf.get("msg.to.hashmap.bolt.parallelism.hint").toString()))
						.setNumTasks(Integer.parseInt(conf.get("msg.to.hashmap.bolt.num.tasks").toString()))
						.localOrShuffleGrouping("spout-00-gn-kafka");						
				//2 bolt-10-20
						builder.setBolt("bolt-10-20-hmset2redis", new NewHmset2RedisBolt(), 
								Integer.parseInt(conf.get("hmset.to.redis.bolt.parallelism.hint").toString()))
								.setNumTasks(Integer.parseInt(conf.get("hmset.to.redis.bolt.num.tasks").toString()))
								.localOrShuffleGrouping("bolt-00-10-msg2hashMap");
						
						
						
						//1 bolt-00-11
						builder.setBolt("bolt-00-11-msg2KV",new Msg2KVBolt(1,conf.get("gn.data.split.str").toString()),
								Integer.parseInt(conf.get("msg.to.hashmap.bolt.parallelism.hint").toString()))
								.setNumTasks(Integer.parseInt(conf.get("msg.to.hashmap.bolt.num.tasks").toString()))
								.localOrShuffleGrouping("spout-00-gn-kafka");		
						//2 bolt-11-21
						builder.setBolt("bolt-11-21-muti2redis", new MultiRedisStoreBolt(new RedisStoreMapper(new RedisDataDescription(RedisDataType.HASHMAP, conf.get("gn.data.split.str").toString(), conf.get("gn.data.schema.fileds").toString(),null,null))), 
								Integer.parseInt(conf.get("hmset.to.redis.bolt.parallelism.hint").toString()))
								.setNumTasks(Integer.parseInt(conf.get("hmset.to.redis.bolt.num.tasks").toString()))
								.localOrShuffleGrouping("bolt-00-11-msg2KV");
		
	}

}




//kafka producer
//new KafkaProducer("test_topic").produce();		
		

//conf.setMessageTimeoutSecs(100);
//String[] zkServers= { "192.168.1.102", "192.168.103", "192.168.104" };
//String[] zkServers= ConfigUtil.getStringArrayValue(conf, "storm.zookeeper.servers.ip");
//conf.setNumWorkers(12);
// 输出统计指标值到日志文件中
//conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);