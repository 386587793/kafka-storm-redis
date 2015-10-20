package com.nlsoft.bi3.bigdata.common.db.redis.container;

import redis.clients.jedis.commands.JedisClusterCommands;
//import redis.clients.jedis.commands.JedisCommands;

/**
 * Interfaces for containers which stores instances implementing JedisCommands.
 */
public interface JedisCommandsInstanceContainer {
    /**
     * Borrows instance from container.
     * @return instance which implements JedisCommands
     */
	JedisClusterCommands getInstance();

    /**
     * Returns instance to container.
     * @param jedisCommands borrowed instance
     */
    void returnInstance(JedisClusterCommands jedisClusterCommands);
}
