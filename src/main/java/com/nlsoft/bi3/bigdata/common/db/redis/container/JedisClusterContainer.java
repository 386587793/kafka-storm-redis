package com.nlsoft.bi3.bigdata.common.db.redis.container;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.commands.JedisClusterCommands;

import java.io.Closeable;

/**
 * Container for managing JedisCluster.
 * <p/>
 * Note that JedisCluster doesn't need to be pooled since it's thread-safe and it stores pools internally.
 */
public class JedisClusterContainer implements JedisCommandsInstanceContainer, Closeable {

    private JedisCluster jedisCluster;

    /**
     * Constructor
     * @param jedisCluster JedisCluster instance
     */
    public JedisClusterContainer(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JedisClusterCommands getInstance() {
        return this.jedisCluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnInstance(JedisClusterCommands jedisCommands) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        this.jedisCluster.close();
    }
}
