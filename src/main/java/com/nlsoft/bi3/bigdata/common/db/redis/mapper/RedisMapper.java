/**
 * @see apache storm storm-redis
 */
package com.nlsoft.bi3.bigdata.common.db.redis.mapper;

/**
 * RedisMapper is for defining data type for querying / storing from / to Redis.
 */
public interface RedisMapper {
    /**
     * Returns descriptor which defines data type.
     * @return data type descriptor
     */
    RedisDataDescription getDataDescription();
}
