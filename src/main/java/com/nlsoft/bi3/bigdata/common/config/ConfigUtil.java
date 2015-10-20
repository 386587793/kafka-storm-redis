/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月28日 下午17:44:20
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.common.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;

/**
 * 获取storm配置值
 * 
 * @author
 */
public class ConfigUtil {
	
	ConfigUtil() {
	}
	
	/**
	 * 
	 * @param stormConf 配置对象
	 * @param key 
	 * @param defaultValue 默认值
	 * @return
	 */
	public static String getStrVal(Config stormConf, String key,
			String defaultValue) {
		Object config = stormConf.get(key);

		if (config == null) {
			return defaultValue;
		}

		return config.toString();
	}
	/**
	 * 
	 * @param stormConf 配置对象
	 * @param key 
	 * @param defaultValue 默认值
	 * @return
	 */
	public static String getStrVal(Config stormConf, String key) {
		Object config = stormConf.get(key);
		return config.toString();
	}

	/**
	 * 
	 * @param stormConf
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static int getIntVal(Config stormConf, String key, int defaultValue) {
		Object config = stormConf.get(key);

		if (config == null) {
			return defaultValue;
		}

		return Integer.parseInt(config.toString());
	}
	/**
	 * 
	 * @param stormConf
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static boolean getBooleanVal(Config stormConf, String key, boolean defaultValue) {
		Object config = stormConf.get(key);

		if (config == null) {
			return defaultValue;
		}

		return Boolean.parseBoolean(config.toString());
	}


	/**
	 * 
	 * @param stormConf
	 * @param key
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static List<String> getStringListValue(Config stormConf, String key) {
		List<String> configList = (List<String>) stormConf.get(key);

		if (configList == null) {
			configList = new ArrayList<String>();
		}

		return configList;
	}
	/**
	 * 
	 * @param stormConf
	 * @param key
	 * @return
	 */
	public static String[] getStrArrayVal(Config stormConf, String key) {
		List<String> configList = getStringListValue(stormConf, key);
		return configList.toArray(new String[configList.size()]);
	}
	
	/**
	 * 
	 * @param stormConf
	 * @param key
	 * @return
	 */
	@SuppressWarnings({ "rawtypes" })
	public static Map getMapValue(Config stormConf, String key) {
		Map configMap = (Map) stormConf.get(key);
		return configMap;
	}
}
