/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月28日 下午17:44:20
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.common.fs.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.TimeCacheMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.nlsoft.bi3.bigdata.common.fs.filter.HdfsPathFilter;

/**
 * 用于操作HDFS文件流
 */
public class HdfsReader extends FSReader {
	private static Logger LOG = LoggerFactory.getLogger("HdfsReader");
	private FileSystem fs = null;

	public void createFs(String path) throws IOException {
		createFs(new Path(path));
	}

	public void createFs(Path path) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.addResource(Thread.currentThread().getContextClassLoader()
				.getResource("resources/core-site.xml"));
		conf.addResource(Thread.currentThread().getContextClassLoader()
				.getResource("resources/hdfs-site.xml"));
		// conf.addResource(new Path("/home/hadoop/core-site.xml"));
		// conf.addResource(new Path("/home/hadoop/hdfs-site.xml"));
		try {
			// 使用getFileSystem获得这条路正确的文件系统类型.
			fs = path.getFileSystem(conf);
		} catch (IOException e) {
			LOG.error("Failed to read hadoop path: \n" + toAbsPath(path), e);
			throw new IOException(e);
		}
	}

	private static String toAbsPath(Path path) {
		String pathString = path.toString();
		boolean isFile = pathString.startsWith("file:");
		boolean isHdfs = pathString.startsWith("hdfs:");
		if (isFile || isHdfs) {
			return pathString.substring(5, pathString.length()).replace(
					"//mycluster", "");
		}
		if (!pathString.startsWith("/"))
			throw new IllegalArgumentException("Invalid path, ensure path"
					+ "starts with either `file://`, `hdfs://`, or `/`. Path: "
					+ pathString);
		return pathString;
	}

	public FSDataInputStream open(Path path) throws IOException {
		if (fs == null)
			createFs(path);
		return fs.open(path);
	}

	@Override
	public InputStream getStream(String path) throws IOException {
		return open(new Path(path));
	}

	@Override
	public String[] getFileList(String path, String filter, int limit,
			boolean sortByModificationTime) throws IOException {
		if (fs == null)
			createFs(path);

		List<FileStatus> pathList = Arrays.asList(fs.globStatus(new Path(path
				+ "/*"), new HdfsPathFilter(filter)));

		// 排序
		if (sortByModificationTime)
			Collections.sort(pathList, new Comparator<FileStatus>() {
				public int compare(FileStatus f1, FileStatus f2) {
					if (f1.getModificationTime() < f2.getModificationTime()) {
						return 1;
					} else if (f1.getModificationTime() == f2
							.getModificationTime()) {
						return 0;
					} else {
						return -1;
					}

				}
			});

		int size = pathList.size() > limit ? limit : pathList.size();
		String[] fileList = new String[size];
		for (int i = 0; i < size; i++) {
			fileList[i] = toAbsPath(pathList.get(i).getPath());
		}
		return fileList;
	}

	@Override
	public FileSystem getFs(String path) throws IOException {
		if (fs == null)
			createFs(path);
		return fs;
	}
}

/*
 * Path[] listedPaths = FileUtil.stat2Paths(status); LOG.info(" filter: "
 * +filter); for (Path p : listedPaths) { LOG.info(" listPath: " +p); }
 */
// for (int i = 0; i < status.length; i++) {
// FileStatus status[] = fs.listStatus(new Path(path));

// String stringArr[] = new String[status.length];