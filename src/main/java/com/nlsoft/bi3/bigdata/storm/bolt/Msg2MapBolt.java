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
public class Msg2MapBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(Msg2MapBolt.class);
	private boolean isDebug=false;
	private static final String DEFAULT_STRING_DELIM = ",";
	private static final String DEFAULT_SECEMA_DELIM = ",";
	
	private String stringDelim;// 数据分隔符
	private String schemaDelim;// 解析分隔符
	private String schema;//解析,设计为tablename,filed1,filed2……

	public Msg2MapBolt(String schema) {
		this(schema, DEFAULT_STRING_DELIM);
	}

	public Msg2MapBolt(String schema, String stringDelim) {
		this(schema, stringDelim, DEFAULT_SECEMA_DELIM);
	}

	public Msg2MapBolt(String schema, String stringDelim, String schemaDelim) {
		this.stringDelim = stringDelim;
		this.schemaDelim = schemaDelim;
		this.schema = schema;
	}
	
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String fields[] = tuple.getString(0).split(this.stringDelim);
		String schemas[]=schema.split(this.schemaDelim);
		if (isDebug) {
			LOG.info("msg-- SourceComponent:{} tuple:{}",tuple.getSourceComponent(), tuple.getString(0));
			LOG.info("schemaDelim:" + this.stringDelim);
			LOG.info("isEmit-- fields.length:{} schemas.length:{}",fields.length, schemas.length);
		}
		Map <String,String> emitData=new HashMap<String,String>();
		// 过滤不匹配数据
		if (fields.length == schemas.length) {
			for (int i = 0; i < fields.length; i++) {
				emitData.put(schemas[i], fields[i]);
			}
			collector.emit(new Values(emitData));
			//collector.emit(new Values(new RedisDataDescription(null, null)));
			//collector.emit(fields[1], new Values(Arrays.asList(fields)));
			// collector.emit(new Values(fields[0], fields[1]));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msg"));
		//declarer.declare(new Fields(this.schema.split(this.schemaDelim)));
	}
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		isDebug = (boolean) stormConf.get("topology.isdebug");
	}

}