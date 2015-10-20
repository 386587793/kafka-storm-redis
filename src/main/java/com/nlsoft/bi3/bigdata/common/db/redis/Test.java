/**
* @author: gsw
* @version: 1.0
* @CreateTime: 2015年10月14日 下午1:56:23
* @Description: 无
*/
package com.nlsoft.bi3.bigdata.common.db.redis;

import java.util.ArrayList;

import redis.clients.jedis.JedisCluster;

public class Test {

	public static void main(String[] args) {
		ArrayList<String> places = new ArrayList<String>();
		places.add("192.168.1.101:7001");
		places.add("192.168.1.101:7002");
		places.add("192.168.1.101:7003");
		places.add("192.168.1.101:8001");
		places.add("192.168.1.101:8002");
		places.add("192.168.1.101:8003");
		JedisCluster jc=new RedisCluster(places).getInstance();
		
		//redisCluster.
	}

}
