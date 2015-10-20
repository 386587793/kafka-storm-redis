package com.nlsoft.bi3.bigdata.common.db.redis.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class DefaultJedisPoolConfig extends GenericObjectPoolConfig {
	public DefaultJedisPoolConfig() {
		// defaults to make your life with connection pool easier :)
		setTestWhileIdle(true);
		//setMinEvictableIdleTimeMillis(60000);
		//setTimeBetweenEvictionRunsMillis(30000);
		setNumTestsPerEvictionRun(-1);

		setMaxIdle(32);// 最大能够保持idel状态的对象数
		setMinIdle(1);
		setMaxTotal(128);// 最大分配的对象数
		setMaxWaitMillis(1000);
		setMinEvictableIdleTimeMillis(900000);// 空闲连接多长时间后会被收回
		setTimeBetweenEvictionRunsMillis(100000);// 多长时间检查一次空闲的连接
	}
}
