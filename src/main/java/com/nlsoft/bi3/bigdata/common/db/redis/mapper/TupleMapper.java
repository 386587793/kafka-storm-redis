/**
 * @see apache storm storm-redis
 */
package com.nlsoft.bi3.bigdata.common.db.redis.mapper;

import backtype.storm.tuple.ITuple;

import java.io.Serializable;

/**
 * TupleMapper defines how to extract key and value from tuple for Redis.
 */
public interface TupleMapper extends Serializable {
    /**
     * Extracts key from tuple.
     * @param tuple source tuple
     * @return key
     */
    String getKeyFromTuple(ITuple tuple);

    /**
     * Extracts value from tuple.
     * @param tuple source tuple
     * @return value
     */
    String getValueFromTuple(ITuple tuple);
}
