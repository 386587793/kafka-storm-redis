/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年10月17日 下午4:30:41
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.storm.topo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
	public static void main(String[] args) {
		String str="14799394559|0|64790|32461|862817020378260|532|2014-06-07 20:03:02.0534660|2014-06-07 20:03:02.0558620|1|72|84|156|1|10.8.4.211|218.202.152.131|0||CMNET|460079198710250|221.177.180.44|223.103.30.33||13247|53|0|0||andmlb.tj.ijinshan.com (A)(Host address)";
		System.out.println(getCharacterPosition(str,1));
		System.out.println(str.substring(getCharacterPosition(str,1)-11, getCharacterPosition(str,1)));
	}

	public static int getCharacterPosition(String string,int index) {
		// 这里是获取"/"符号的位置
		Matcher slashMatcher = Pattern.compile("\\|").matcher(string);
		int mIdx = 0;
		while (slashMatcher.find()) {
			mIdx++;
			// 当"/"符号第三次出现的位置
			if (mIdx == index) {
				break;
			}
		}
		return slashMatcher.start();
	}

}
