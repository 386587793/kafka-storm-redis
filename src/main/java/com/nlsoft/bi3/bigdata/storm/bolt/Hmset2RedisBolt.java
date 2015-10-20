/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月28日 下午17:44:20
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCluster;

import com.nlsoft.bi3.bigdata.common.config.ConfigUtil;
import com.nlsoft.bi3.bigdata.common.db.redis.RedisCluster;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.exceptions.JedisClusterException;

/**
 * 用于持久化数据
 */
public class Hmset2RedisBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 7239045099254159871L;
	private static Logger LOG = LoggerFactory.getLogger("Hmset2RedisBolt");
	private boolean isDebug=false;
	private JedisCluster redisCluster;
	private String dataKey;
	public static final String DEFAULT_DATA_KEY = "MSISDN";

	public Hmset2RedisBolt() {
		this(DEFAULT_DATA_KEY);
	}

	public Hmset2RedisBolt(String dataKey) {
		this.dataKey = dataKey;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		isDebug = (boolean) stormConf.get("topology.isdebug");
		redisCluster = new RedisCluster(
				(List<String>) stormConf.get("redis.cluster.nodes.hostandport"))
				.getInstance();
	}

	@SuppressWarnings("unchecked")
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (isDebug) {
			LOG.info("SourceComponent:" + tuple.getSourceComponent());
			LOG.info("tuple:" + tuple.getValue(0).toString());
		}

		Map<String, String> dataValue = (Map<String, String>) tuple.getValue(0);
		if (dataValue != null && dataValue.get(this.dataKey).trim() != ""
				&& dataValue.get(this.dataKey) != null) {
		try {
				redisCluster.hmset(dataValue.get(this.dataKey), dataValue);
				redisCluster.incrBy("total", 1);
				//LOG.info("[Hmset2RedisBolt] msg: {}",dataValue.get(this.dataKey));			
		} catch (JedisClusterException e) {
			LOG.error("[Hmset2RedisBolt]：RedisCluster JedisClusterException");
			try {
				Thread.sleep(1000);
				LOG.info ("[Hmset2RedisBolt]：RedisCluster JedisClusterException");
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
		
		}		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields(""));
	}

	@Override
	public void cleanup() {
		redisCluster.close();
	}
}