/**
* @author: gsw
* @version: 1.0
* @CreateTime: 2015年10月16日 上午10:15:41
* @Description: 无
*/
package com.nlsoft.bi3.bigdata.common.db.redis.mapper;

import backtype.storm.tuple.ITuple;

public class RedisStoreMapper implements IRedisStoreMapper  {

	private static final long serialVersionUID = 1L;
	private RedisDataDescription description;
	
	/**
	 * Constructor
	 * @param description
	 */
	public RedisStoreMapper(RedisDataDescription description) {
		this.description = description;

	}

	@Override
	public String getKeyFromTuple(ITuple tuple) {		
		return tuple.getStringByField("key");
	}

	@Override
	public String getValueFromTuple(ITuple tuple) {
		return tuple.getStringByField("value");
	}

	@Override
	public RedisDataDescription getDataDescription() {	
		return this.description;
	}

}
