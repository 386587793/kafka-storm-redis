/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月28日 下午17:44:20
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.storm.bolt;

import java.util.Map;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nlsoft.bi3.bigdata.common.db.redis.bolt.AbstractRedisBolt;
import redis.clients.jedis.commands.JedisClusterCommands;

public class NewHmset2RedisBolt extends AbstractRedisBolt {

	private static final long serialVersionUID = 7239045099254159871L;
	private static Logger LOG = LoggerFactory
			.getLogger(NewHmset2RedisBolt.class);
	private String dataKey;
	public static final String DEFAULT_DATA_KEY = "MSISDN";

	public NewHmset2RedisBolt() {
		this(DEFAULT_DATA_KEY);
	}

	public NewHmset2RedisBolt(String dataKey) {
		this.dataKey = dataKey;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		JedisClusterCommands command = null;
		Map<String, String> dataValue = (Map<String, String>) input.getValue(0);
		if (dataValue != null && dataValue.get(this.dataKey).trim() != ""
				&& dataValue.get(this.dataKey) != null) {
			try {
				command = getInstance();
				command.hmset(dataValue.get(this.dataKey), dataValue);
				collector.ack(input);
			} catch (Exception e) {
				this.collector.reportError(e);
				this.collector.fail(input);
			} finally {
				returnInstance(command);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields(""));
	}
}