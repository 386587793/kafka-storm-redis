package com.nlsoft.bi3.bigdata.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * 
 * @ClassName: Msg2FieldsBolt
 * @Description: 按照配置文件进行分割数据
 *               </p>
 * 
 * <pre>
 * </pre>
 */
public class Msg2KVBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(Msg2KVBolt.class);
	private boolean isDebug=false;
	private static final String DEFAULT_STRING_DELIM = "\\|";
	
	private String fieldDelim;// 数据分隔符	
	private  int keyIndex ;//

	public Msg2KVBolt(int keyIndex) {
		this(keyIndex, DEFAULT_STRING_DELIM);
	}

	public Msg2KVBolt(int keyIndex, String fieldDelim) {
		this.keyIndex=keyIndex;
		this.fieldDelim=fieldDelim;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		isDebug = (boolean) stormConf.get("topology.isdebug");
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String value = tuple.getString(0);
		int endIndex = getCharacterPosition(value, this.fieldDelim,
				this.keyIndex);
		String key = value.substring(endIndex - 11, endIndex);
		if (key.matches("^[0-9]*$"))
			collector.emit(new Values(key, value));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","value"));
	}
	
	protected int getCharacterPosition(String string,String subString,int n) {
		// 这里是获取subString 的位置
		Matcher slashMatcher = Pattern.compile(subString).matcher(string);
		int mIdx = 0;
		while (slashMatcher.find()) {
			mIdx++;
			// 当subString 第n次出现的位置
			if (mIdx == n) {
				break;
			}
		}
		return slashMatcher.start();
	}

}