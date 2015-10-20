/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月28日 下午17:44:20
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.common.fs;

import com.nlsoft.bi3.bigdata.common.fs.reader.FSReader;
import com.nlsoft.bi3.bigdata.common.fs.reader.HdfsReader;
import com.nlsoft.bi3.bigdata.common.fs.reader.LocalReader;

/**
 * file system 生成器类 通过从一个配置参数,创建新的FS对象。
 *
 */

public class FSFactory {
	/**
	 * 获取file system 实例
	 * 默认返回 本地文件系统
	 * @param type
	 * @return
	 */
	public static FSReader get(FSReader.FS type) {
		switch (type) {
		case HDFS:
			return new HdfsReader();
		case LOCAL:
			return new LocalReader();
		default:
			return new HdfsReader();
		}
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static FSReader get(String type) {
		try {
			return get(FSReader.FS.valueOf(type));
		} catch (ClassCastException e) {
			throw new ClassCastException(
					"Failed to parse FSReader type. Are you "
							+ "using the FSReader.FS enum? "
							+ e.getLocalizedMessage());
		}
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static FSReader get(Object type) {
		return get(type.toString());
	}
}
