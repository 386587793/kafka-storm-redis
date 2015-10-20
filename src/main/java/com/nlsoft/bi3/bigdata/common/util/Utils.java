package com.nlsoft.bi3.bigdata.common.util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {
	
	
	
	/**
	 */
	public static boolean renameFile(String src, String extention, FileSystem fs) {
		boolean flag = false;
		try {
			// 重命名文件
			flag = fs.rename(new Path(src), new Path(src + extention));
		} catch (FileNotFoundException e) {
			flag = false;
			e.printStackTrace();
		} catch (IOException e) {
			flag = false;
			e.printStackTrace();
		}
		return flag;
	}
	
	
	/**
	 * 
	 * @param timeStr 转换格式 2015-01-28 19:20:45.607
	 * @return 返回偏移的时间（毫秒）
	 */
	public static long getGMT(String timeStr) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date date = new Date();
		try {
			date = sdf.parse(timeStr);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date.getTime();
	}

	
	/**
	 * 通过指定路径将配置文件读入
	 * 
	 * @param path
	 * @return
	 */
	public static void addProfile(String filePath,HashMap<String, Object> conf) {
		
		File file = new File(filePath);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempStr = null;
			while ((tempStr = reader.readLine()) != null) {
				if (tempStr.startsWith("#"))
					continue;
				String[] kv = null;
				kv = tempStr.split("=");
				if (kv.length == 2)
					conf.put(kv[0].trim(), kv[1].trim());
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

	


}
