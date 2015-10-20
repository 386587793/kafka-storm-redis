/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年8月9日 上午11:28:19
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.common.fs.filter;

import java.io.File;
import java.io.FileFilter;

public class LocalFileFilter implements FileFilter {

	private final String filter;

	public LocalFileFilter(String filter) {
		this.filter = filter;
	}

	@Override
	public boolean accept(File file) {
		if (file.isDirectory())
			return true;
		else {
			String name = file.getName();
			if (name.endsWith(filter))
				return true;
			else
				return false;
		}

	}
}
