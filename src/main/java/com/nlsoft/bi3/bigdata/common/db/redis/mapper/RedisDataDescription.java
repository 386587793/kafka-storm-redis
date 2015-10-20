package com.nlsoft.bi3.bigdata.common.db.redis.mapper;

import java.io.Serializable;

/**
 * descript redis data<p>
 * add more redis data type support<P>
 * add more custom to descript redis data and define data<P>
 * @see org.apache.storm.redis.common.mapper.RedisDataTypeDescription
 */
public class RedisDataDescription implements Serializable {

	private static final long serialVersionUID = 1L;

	public enum RedisDataType {
		STRING, STRING_NX,STRING_EX,
		HASH, HASHMAP, 
		R_LIST, L_LIST, R_LIST_X, L_LIST_X, 
		SET, SORTED_SET,
		HYPER_LOG_LOG
	}

	private RedisDataType dataType;
	private String additionalKey;
	private String delim;
	private String schema;
	private Integer seconds;

	/**
	 * Constructor
	 * @param dataType
	 */
	public RedisDataDescription(RedisDataType dataType) {
		this(dataType, null, null, null,null);
	}

	/**
	 * Constructor
	 * @param dataType
	 * @param delim
	 * @param schema
	 * @param additionalKey
	 * @param seconds
	 */
	public RedisDataDescription(RedisDataType dataType, String delim,
			String schema, String additionalKey, Integer seconds) {
		this.dataType = dataType;	
		this.delim = delim;
		this.schema = schema;
		this.additionalKey = additionalKey;
		this.seconds=seconds;

		if (dataType == RedisDataType.STRING_EX) {
			if (seconds == null) {
				throw new IllegalArgumentException(
						" EX String  should have seconds");
			}
		} else if (dataType == RedisDataType.HASH
				|| dataType == RedisDataType.SORTED_SET) {
			if (additionalKey == null) {
				throw new IllegalArgumentException(
						"Hash and Sorted Set should have additional key");
			}
		} else if (dataType == RedisDataType.R_LIST_X
				|| dataType == RedisDataType.L_LIST_X) {
			if (delim == null) {
				throw new IllegalArgumentException(
						"rlsitx/llistx should have value delim");
			}
		} else if (dataType == RedisDataType.HASHMAP) {
			if (delim == null || schema == null) {
				throw new IllegalArgumentException(
						"Hashmap should have value delim and schema");
			}
		}
	}

	/**
	 * 
	 * @return data schema
	 */
	public String getSchema() {
		return schema;
	}

	/**
	 * Returns defined data type.
	 * 
	 * @return data type
	 */
	public RedisDataType getDataType() {
		return dataType;
	}

	public String getDelim() {
		return delim;
	}

	/**
	 * Returns defined additional key.
	 * 
	 * @return additional key
	 */
	public String getAdditionalKey() {
		return additionalKey;
	}

	/**
	 * @return the seconds
	 */
	public Integer getSeconds() {
		return seconds;
	}


}
