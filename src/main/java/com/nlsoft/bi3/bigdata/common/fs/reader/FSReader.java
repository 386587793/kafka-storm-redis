/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月28日 下午17:44:20
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.common.fs.reader;

//import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang.NotImplementedException;

import backtype.storm.utils.TimeCacheMap;

/**
 * 创建一个抽象类与底层文件系统进行交互 例如: UNIX 文件系统,HDFS
 * 
 * @see com.nlsoft.bi3.bigdata.common.fs.HdfsReader
 * @see com.nlsoft.bi3.bigdata.common.fs.LocalReader
 */
public abstract class FSReader {
	public enum FS {
		LOCAL, HDFS
	}

	/**
	 * 打开文件系统的 DataInputStream 操作完成时注意关闭
	 * 
	 * @throws IOException
	 */
	public InputStream getStream(String path) throws IOException {
		throw new NotImplementedException();
	}

	/**
	 * 取得目录下的文件列表
	 * @param path 目录
	 * @param filter 过滤规则
	 * @param limit 文件数量限制
	 * @param sortByModificationTime 是否按修改时间排序
	 * @return
	 * @throws IOException
	 */
	public String[] getFileList(String path, String filter, int limit,
			boolean sortByModificationTime) throws IOException {
		throw new NotImplementedException();
	}

	public Object getFs(String path) throws IOException {
		return path;
	}

}
