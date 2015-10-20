/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年8月7日 下午2:14:16
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.common.fs.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class HdfsPathFilter implements PathFilter {
	private final String regex;

	public HdfsPathFilter(String regex) {
		this.regex = regex;
	}

	/**
	 * accept方法里面的return !path.toString().matches(regex);
	 * 可以看出来，就是将匹配的全部去除掉
	 * 如果改为return path.toString().matches(regex)
	 * 就是将匹配regex的Path输出，将不匹配的去除。
	 */
	@Override
	public boolean accept(Path path) {
		return path.toString().matches(regex);
	}

}