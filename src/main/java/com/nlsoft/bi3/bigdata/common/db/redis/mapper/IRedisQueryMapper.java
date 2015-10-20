package com.nlsoft.bi3.bigdata.common.db.redis.mapper;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;

import java.util.List;

/**
 * RedisStoreMapper is for defining spec. which is used for querying value from Redis and converting response to tuple.
 */
public interface  IRedisQueryMapper extends TupleMapper, RedisMapper {
    /**
     * Converts return value from RIdis to a list of storm values that can be emitted.
     * @param input the input tuple.
     * @param value Redis query response value. Can be String, Boolean, Long regarding of data type.
     * @return a List of storm values that can be emitted. Each item in list is emitted as an output tuple.
     */
    List<Values> toTuple(ITuple input, Object value);

    /**
     * declare what are the fields that this code will output.
     * @param declarer OutputFieldsDeclarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
