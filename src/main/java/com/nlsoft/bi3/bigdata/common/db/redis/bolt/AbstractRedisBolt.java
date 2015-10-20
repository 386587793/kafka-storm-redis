package com.nlsoft.bi3.bigdata.common.db.redis.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import redis.clients.jedis.commands.JedisClusterCommands;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nlsoft.bi3.bigdata.common.db.redis.config.JedisClusterConfig;
import com.nlsoft.bi3.bigdata.common.db.redis.container.JedisCommandsContainerBuilder;
import com.nlsoft.bi3.bigdata.common.db.redis.container.JedisCommandsInstanceContainer;

/**
 * AbstractRedisBolt class is for users to implement custom bolts which makes interaction with Redis.
 * <p/>
 * Due to environment abstraction, AbstractRedisBolt provides JedisCommands which contains only single key operations.
 * <p/>
 * Custom Bolts may want to follow this pattern:
 * <p><blockquote><pre>
 * JedisCommands jedisCommands = null;
 * try {
 *     jedisCommand = getInstance();
 *     // do some works
 * } finally {
 *     if (jedisCommand != null) {
 *         returnInstance(jedisCommand);
 *     }
 * }
 * </pre></blockquote>
 *
 */
// TODO: Separate Jedis / JedisCluster to provide full operations for each environment to users
public abstract class AbstractRedisBolt extends BaseRichBolt {
	
	private static Logger LOG = LoggerFactory.getLogger(AbstractRedisBolt.class);
    protected OutputCollector collector;

    private transient JedisCommandsInstanceContainer container;

    private JedisClusterConfig jedisClusterConfig;

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {
		// FIXME: stores map (stormConf), topologyContext and expose these to derived classes
		this.collector = collector;
		Set<InetSocketAddress> nodes=null;
		//nodes.add(new InetSocketAddress("", 0));
		int timeout=15000;
		int maxRedirections=100;
		try {
			nodes = toNodes((List<String>) map
					.get("redis.cluster.nodes.hostandport"));
			timeout = Integer.parseInt((String) map
					.get("redis.cluster.connect.timeout"));
			maxRedirections = Integer.parseInt((String) map
					.get("redis.cluster.connect.max_redirections"));
		} catch (ClassCastException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}	
		if (nodes != null) {
			this.jedisClusterConfig = new JedisClusterConfig(nodes, timeout,
					maxRedirections);
			if (jedisClusterConfig != null) {
				this.container = JedisCommandsContainerBuilder
						.build(jedisClusterConfig);
			} else {
				LOG.error("配置找不到");
			}
		} else {
			LOG.error("nodes is null");
		}
	}

    /**
     * Borrow JedisCommands instance from container.<p/>
     * JedisCommands is an interface which contains single key operations.
     * @return implementation of JedisCommands
     * @see JedisCommandsInstanceContainer#getInstance()
     */
    protected JedisClusterCommands getInstance() {
        return this.container.getInstance();
    }

    /**
     * Return borrowed instance to container.
     * @param instance borrowed object
     */
    protected void returnInstance(JedisClusterCommands instance) {
        this.container.returnInstance(instance);
    }
    
	protected Set<InetSocketAddress> toNodes(List<String> hostandport) {
		Set<InetSocketAddress> nodes = null;
		if (hostandport != null) {
			if (hostandport.size() > 0) {
				nodes = new HashSet<InetSocketAddress>();
				for (String str : hostandport) {
					String[] node = str.split(":");
					if (node.length == 2) {
						nodes.add(new InetSocketAddress(node[0], node[1]
								.matches("^[0-9]*$") ? Integer
								.parseInt(node[1]) : 6379));
					} else {
						LOG.error("hostandport 配置项有误");
					}
				}
			} else {
				LOG.error("hostandport 配置项有误");
			}
		} else {
			LOG.error("hostandport 配置项找不到");
		}
		return nodes;
	}
}
