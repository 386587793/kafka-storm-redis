package com.nlsoft.bi3.bigdata.common.fs.reader;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.nlsoft.bi3.bigdata.common.fs.filter.LocalFileFilter;

import backtype.storm.utils.TimeCacheMap;


public class LocalReader extends FSReader {

	/**
	 * 用于操作本地文件流
	 *
	 * @throws IOException
	 */
	@Override
	public InputStream getStream(String path) throws IOException {
		return new BufferedInputStream(new FileInputStream(path));
	}

	/**
	 * 取得路径下的文件列表
	 *
	 * @return
	 */
	@Override
	public String[] getFileList(String path, String filter, int limit,
			boolean sortByModificationTime) throws IOException {

		List<File> pathList = Arrays.asList(new File(path)
				.listFiles(new LocalFileFilter(filter)));
		// 排序
		if (sortByModificationTime)
			Collections.sort(pathList, new Comparator<File>() {
				public int compare(File file, File newFile) {
					if (file.lastModified() < newFile.lastModified()) {
						return 1;
					} else if (file.lastModified() == newFile.lastModified()) {
						return 0;
					} else {
						return -1;
					}
				}
			});

		int size = pathList.size() > limit ? limit : pathList.size();
		String[] fileList = new String[size];
		for (int i = 0; i < size; i++) {
			fileList[i] = pathList.get(i).getAbsolutePath();
		}
		return fileList;
	}
}
