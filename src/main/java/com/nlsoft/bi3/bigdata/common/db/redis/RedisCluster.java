/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月30日 上午10:53:25
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.common.db.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * http://redis.io/topics/cluster-tutorial The startup nodes don't need to be
 * all the nodes of the cluster. The important thing is that at least one node
 * is reachable. Also note that redis-rb-cluster updates this list of startup
 * nodes as soon as it is able to connect with the first node. You should expect
 * such a behavior with any other serious client 
 * 
 */
public class RedisCluster {
	private static JedisCluster redisCluster;
	private static Logger LOG = LoggerFactory.getLogger("RedisCluster");
	private static final int DEFAULT_TIMEOUT = 15000;
	private static final int DEFAULT_MAX_REDIRECTIONS = 100;
	private List<String> hostAndport;
	public static final List<String> DEFAULT_HOSTANDPORT = new ArrayList<String>(Arrays.asList("127.0.0.1:6379"));

	public RedisCluster() {
		this(DEFAULT_HOSTANDPORT);
	}

	public RedisCluster(List<String> hostAndport) {
		this.hostAndport = hostAndport;
	}

	public synchronized JedisCluster getInstance() {
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();	 
		poolConfig.setMaxIdle(32);//最大能够保持idel状态的对象数
		poolConfig.setMinIdle(1);
		poolConfig.setMaxTotal(128);//最大分配的对象数
		poolConfig.setMaxWaitMillis(1000);	
		poolConfig.setMinEvictableIdleTimeMillis(900000);//空闲连接多长时间后会被收回 
		poolConfig.setTimeBetweenEvictionRunsMillis(100000);//多长时间检查一次空闲的连接 
		// 只给集群里一个实例就可以
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		String[] clusterNode;
		clusterNode = this.hostAndport.get(0).split(":");
		jedisClusterNodes.add(new HostAndPort(clusterNode[0], 
				clusterNode[1].matches("^[0-9]*$") ? Integer.parseInt(clusterNode[1]): 6379));
		redisCluster = new JedisCluster(jedisClusterNodes, DEFAULT_TIMEOUT,DEFAULT_MAX_REDIRECTIONS,poolConfig);
		if (redisCluster !=null){
		int offset = 0;
		// 连接异常
		while (redisCluster.getClusterNodes().size() < this.hostAndport.size()
				&& offset < this.hostAndport.size()) {
			LOG.warn("集群节点数量异常:{},尝试重新建立连接{},第{}次", redisCluster.getClusterNodes().size(), this.hostAndport.get(offset),offset + 1);
			jedisClusterNodes.clear();
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			clusterNode = this.hostAndport.get(offset).split(":");
			jedisClusterNodes.add(new HostAndPort(clusterNode[0],
					clusterNode[1].matches("^[0-9]*$") ? Integer
							.parseInt(clusterNode[1]) : 6379));
			redisCluster = new JedisCluster(jedisClusterNodes, DEFAULT_TIMEOUT,DEFAULT_MAX_REDIRECTIONS,poolConfig);
			offset++;
		}
		if (redisCluster.getClusterNodes().size() == this.hostAndport.size()) {
			LOG.info("连接成功.集群节点数量:{}", redisCluster.getClusterNodes().size());
			for (Entry<String, JedisPool> e : redisCluster.getClusterNodes()
					.entrySet()) {
				LOG.info("节点 " + e.getKey());
				//e.getValue().getResource().info();
			}
		} else {
			LOG.error("连接失败.集群节点数量:{}", redisCluster.getClusterNodes().size());
		}
		}
		return redisCluster;
	}
}

