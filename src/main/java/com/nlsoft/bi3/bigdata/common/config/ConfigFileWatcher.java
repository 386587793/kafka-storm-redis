package com.nlsoft.bi3.bigdata.common.config;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import backtype.storm.Config;

/**
 * 配置文件监控
 * 
 */
public class ConfigFileWatcher {
	/** 目标文件 */
	protected File targetFile;
	/** 监控间隔时间. */
	protected long watchInterval;
	/** 最后监控时间. */
	protected long lastWatchTime;
	/** 目标文件最后修改时间. */
	protected long lastModifytime;
	/** file system */
	protected String fs;

	/**
	 *
	 * @param targetPath
	 *            watch 目录
	 * @param watchIntervalSec
	 *            watch 间隔 sec
	 */
	public ConfigFileWatcher(String targetPath, long watchIntervalSec, String fs) {
		this.targetFile = new File(targetPath);
		this.watchInterval = TimeUnit.SECONDS.toMillis(watchIntervalSec);
		this.fs = fs;
	}

	/**
	 * 初始化
	 */
	public void init() {
		// 初始化：最后监控时间，目标文件最后修改时间
		this.lastWatchTime = getNowTime();
		if (this.targetFile.exists())
			this.lastModifytime = this.targetFile.lastModified();
	}

	/**
	 * 
	 * @return 如果监控的目标配置文件发生修改则载入新的配置文件
	 * @throws IOException
	 */
	public Config readIfUpdated() throws IOException {
		long nowTime = getNowTime();
		if ((nowTime - this.lastWatchTime) <= this.watchInterval)
			return null;
		this.lastWatchTime = nowTime;
		if (this.targetFile.exists() == false)
			return null;
		long targetFileModified = this.targetFile.lastModified();
		if (this.lastModifytime >= targetFileModified)
			return null;
		this.lastModifytime = targetFileModified;
		return new ConfigLoader(this.fs).loadStormConfig(this.targetFile.getPath());
	}

	/**
	 * 获取当前时间.
	 * 
	 * @return now time
	 */
	protected long getNowTime() {
		return System.currentTimeMillis();
	}
}
