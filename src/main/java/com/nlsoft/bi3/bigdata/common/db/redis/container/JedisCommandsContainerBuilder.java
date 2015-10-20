package com.nlsoft.bi3.bigdata.common.db.redis.container;

import redis.clients.jedis.JedisCluster;


/**
 * Container Builder which helps abstraction of two env. - single instance or Redis Cluster.
 */
public class JedisCommandsContainerBuilder {

    // FIXME: We're using default config since it cannot be serialized
    // We still needs to provide some options externally
    public static final com.nlsoft.bi3.bigdata.common.db.redis.config.DefaultJedisPoolConfig DEFAULT_POOL_CONFIG = new com.nlsoft.bi3.bigdata.common.db.redis.config.DefaultJedisPoolConfig();

    /**
     * Builds container for Redis Cluster environment.
     * @param jedisClusterConfig configuration for JedisCluster
     * @return container for Redis Cluster environment
     */
    public static JedisCommandsInstanceContainer build(com.nlsoft.bi3.bigdata.common.db.redis.config.JedisClusterConfig jedisClusterConfig) {
        JedisCluster jedisCluster = new JedisCluster(jedisClusterConfig.getNodes(), jedisClusterConfig.getTimeout(), jedisClusterConfig.getMaxRedirections(), DEFAULT_POOL_CONFIG);
        return new JedisClusterContainer(jedisCluster);
    }
}
