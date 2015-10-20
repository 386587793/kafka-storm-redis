/**
* @author: gsw
* @version: 1.0
* @CreateTime: 2015年10月17日 下午3:43:47
* @Description: 无
*/
package com.nlsoft.bi3.bigdata.storm.bolt;


import java.util.HashMap;
import com.nlsoft.bi3.bigdata.common.db.redis.bolt.AbstractRedisBolt;
import com.nlsoft.bi3.bigdata.common.db.redis.mapper.RedisDataDescription;
import com.nlsoft.bi3.bigdata.common.db.redis.mapper.RedisStoreMapper;

import redis.clients.jedis.commands.JedisClusterCommands;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MultiRedisStoreBolt  extends AbstractRedisBolt {
	
	private static final long serialVersionUID = 1L;
		private final RedisStoreMapper storeMapper;
	    private final RedisDataDescription.RedisDataType dataType;
	    private final String additionalKey;
	    private final String delim;
	    private final String schema;
	    private final int seconds;

	 
	public MultiRedisStoreBolt(RedisStoreMapper storeMapper) {
		this.storeMapper = storeMapper;

		RedisDataDescription dataDescription = storeMapper.getDataDescription();
		this.dataType = dataDescription.getDataType();
		this.additionalKey = dataDescription.getAdditionalKey();
		this.delim = dataDescription.getDelim();
		this.schema = dataDescription.getSchema();
		this.seconds=dataDescription.getSeconds();
	}

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public void execute(Tuple input) {
	        String key = storeMapper.getKeyFromTuple(input);
	        String value = storeMapper.getValueFromTuple(input);
	        HashMap<String, String> kvs=null;

	        JedisClusterCommands cmd = null;
	        try {
	            cmd = getInstance();

			switch (dataType) {
			case STRING:
				cmd.set(key, value);
				break;
				
			case STRING_NX:
				cmd.set(key, value);
				break;
				
			case STRING_EX:
				cmd.setex(key, seconds, value);
				break;

			case R_LIST:
				cmd.rpush(key, value);
				break;
				
			case L_LIST:
				cmd.lpush(key, value);
				break;
				
			case R_LIST_X:
				cmd.rpushx(key, value.split(this.delim));
				break;
				
			case L_LIST_X:
				cmd.lpushx(key, value.split(this.delim));
				break;

			case HASH:
				cmd.hset(additionalKey, key, value);
				break;
				
			case HASHMAP:
				kvs = toHashMap(value, this.delim, this.schema);
				if (kvs != null)
					cmd.hmset(key, kvs);
				break;
			case SET:
				cmd.sadd(key, value);
				break;

			case SORTED_SET:
				cmd.zadd(additionalKey, Double.valueOf(value), key);

			case HYPER_LOG_LOG:
				cmd.pfadd(key, value);
				break;

			default:
				throw new IllegalArgumentException(
						"Cannot process such data type: " + dataType);
			}

	            collector.ack(input);
	        } catch (Exception e) {
	            this.collector.reportError(e);
	            this.collector.fail(input);
	        } finally {
	            returnInstance(cmd);
	        }
	    }

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    }

	protected HashMap<String, String> toHashMap(String value, String delim,
			String schema) {
		HashMap<String, String> data = null;
		String fields[] = value.split(delim);
		String schemas[] = schema.split(",");
		// 过滤不匹配数据
		if (fields.length == schemas.length) {
			data = new HashMap<String, String>();
			for (int i = 0; i < fields.length; i++) {
				data.put(schemas[i], fields[i]);
			}
		} else {
		}
		return data;
	}
	
	}
