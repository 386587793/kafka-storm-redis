/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月28日 下午17:44:20
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.common.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.scanner.ScannerException;

import com.nlsoft.bi3.bigdata.common.fs.FSFactory;
import com.nlsoft.bi3.bigdata.common.fs.reader.FSReader;

import backtype.storm.Config;

/**
 * <p>
 * @ClassName: ConfigLoader
 * @Description:  配置文件加载类(默认从hdfs读取配置文件)
 * </p>
 * <pre>
 * 文件名的组成：
 * 只要调用loadStormConfig(String filePath) 方法即可
 * 例：new ConfigLoader().loadStormConfig(STORM_CONF_PATH);
 * </pre>
 */
public class ConfigLoader {
	/** 初始化配置文件路径 */
	public static final String INIT_CONFIG_KEY = "topology.init.config.path";
	public static final String DEFAULT_FS = "LOCAL";//分布式文件系统:HDFS, 本地磁盘:LOCAL
	private FSReader fsReader = null;

	/**
	 * 配置文件加载类(默认从hdfs读取配置文件)
	 * 指定yaml文件,生成配置对象 
	 * 只要调用loadStormConfig(String filePath) 方法即可
	 */
	public ConfigLoader() {
		this(DEFAULT_FS);
	}

	public ConfigLoader(String fs) {
		this.fsReader = FSFactory.get(fs);
	}

	/**
	 * 加载配置文件
	 * @see 加载配置文件的同时,会往config中put一条 INIT_CONFIG_KEY 用于对配置文件的监控
	 * 
	 * @param filePath 配置文件路径          
	 * @return storm config 对象
	 * @throws IOException
	 */
	public Config loadStormConfig(String filePath) throws IOException {
		Map<String, Object> yamlConfig = readYaml(filePath);
		Config stormConf = convertYamlToStormConf(yamlConfig);
		/**
		 * 为了追踪配置文件的更新 把加载的配置文件也加到stromConf
		 */
		if (stormConf.containsKey(INIT_CONFIG_KEY) == false) {
			File file = new File(filePath);
			String absolutePath = file.getAbsolutePath();
			stormConf.put(INIT_CONFIG_KEY, absolutePath);
		}
		return stormConf;
	}

	/**
	 * 把yarm file转换为Storm conf 对象
	 * 
	 * @param yamlConf
	 * @return
	 */
	public Config convertYamlToStormConf(Map<String, Object> yamlConf) {
		Config stormConf = new Config();

		for (Entry<String, Object> targetConfigEntry : yamlConf.entrySet()) {
			stormConf.put(targetConfigEntry.getKey(),
					targetConfigEntry.getValue());
		}

		return stormConf;
	}

	/**
	 * 把指定文件转换为yaml 对象
	 * 
	 * @param targetFile
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> readYaml(String targetFile) throws IOException {
		Map<String, Object> configObject = null;
		Yaml yaml = new Yaml();
		InputStream inputStream = null;
		InputStreamReader steamReader = null;
		try {
			inputStream = fsReader.getStream(targetFile);
			steamReader = new InputStreamReader(inputStream, "UTF-8");
			configObject = (Map<String, Object>) yaml.load(steamReader);
		} catch (ScannerException ex) {
			throw new IOException(ex);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
		return configObject;
	}
}
