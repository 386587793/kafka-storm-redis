package com.nlsoft.bi3.bigdata.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.List;

import redis.clients.jedis.commands.JedisClusterCommands;

import com.nlsoft.bi3.bigdata.common.db.redis.bolt.AbstractRedisBolt;
import com.nlsoft.bi3.bigdata.common.db.redis.mapper.IRedisQueryMapper;
import com.nlsoft.bi3.bigdata.common.db.redis.mapper.RedisDataDescription;

/**
 * Basic bolt for querying from Redis and emits response as tuple.
 * <p/>
 * Various data types are supported: STRING, LIST, HASH, SET, SORTED_SET, HYPER_LOG_LOG
 */
public class MutiRedisQueryBolt extends AbstractRedisBolt {
    private final IRedisQueryMapper mapper;
    private final RedisDataDescription.RedisDataType dataType;
    private final String additionalKey;

    /**
     * Constructor for single Redis environment (JedisPool)
     * @param config configuration for initializing JedisPool
     * @param lookupMapper mapper containing which datatype, query key, output key that Bolt uses
     */
    public MutiRedisQueryBolt( IRedisQueryMapper mapper) {

        this.mapper = mapper;

        RedisDataDescription dataDescription = mapper.getDataDescription();
        this.dataType = dataDescription.getDataType();
        this.additionalKey = dataDescription.getAdditionalKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input) {
        String key = mapper.getKeyFromTuple(input);
        Object objectValue;

        JedisClusterCommands cmd = null;
        try {
            cmd = getInstance();

            switch (dataType) {
                case STRING :
                    objectValue = cmd.get(key);
                    break;
                    
                case STRING_EX :
                    objectValue = cmd.get(key);
                    break;
                    
                case STRING_NX :
                    objectValue = cmd.get(key);
                    break;

                case L_LIST:
                    objectValue = cmd.lpop(key);
                    break;
                    
                case R_LIST:
                    objectValue = cmd.rpop(key);
                    break;

                case HASH:
                    objectValue = cmd.hget(additionalKey, key);
                    break;

                case SET:
                    objectValue = cmd.scard(key);
                    break;

                case SORTED_SET:
                    objectValue = cmd.zscore(additionalKey, key);
                    break;

                case HYPER_LOG_LOG:
                    objectValue = cmd.pfcount(key);
                    break;

                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
            }

            List<Values> values = mapper.toTuple(input, objectValue);
            for (Values value : values) {
                collector.emit(input, value);
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
    	mapper.declareOutputFields(declarer);
    }
}
