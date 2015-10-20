/**
 * @author: gsw
 * @version: 1.0
 * @CreateTime: 2015年7月24日 下午2:52:38
 * @Modified: 2015年10月20日 gsw
 * @Description: 无
 */
package com.nlsoft.bi3.bigdata.storm.spout;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.*;

/**
 * 
 * kafka spout factory 
 *
 */
public class KafkaSpoutFactory {

	private BrokerHosts brokerHosts;
	private SpoutConfig spoutConf;
	private KafkaSpout kafkaSpout;
	// 默认参数
	private static final String[] ZKSERVERS = { "nimbus", "supervisor01","supervisor02" }; // 记录Spout读取进度所用的zookeeper的host
	private static final Integer ZKPORT = 2181;// 记录进度用的zookeeper的端口
	private static final String ZKROOT = "/storm";// 进度信息记录于zookeeper的哪个路径下
	private static final String ID = "test-storm-kafka";// 进度记录的id，想要一个新的Spout读取之前的记录，应把它的id设为跟之前的一样。
	private static final Scheme SCHEME = new StringScheme();
	private static Logger LOG = LoggerFactory.getLogger(KafkaSpoutFactory.class);

	/**
	 * Constructor
	 * @param topic
	 */
	public KafkaSpoutFactory(String topic) {
		this(topic, ID);
	}

	/**
	 * Constructor
	 * @param topic
	 * @param spoutId
	 */
	public KafkaSpoutFactory(String topic, String spoutId) {
		this(topic, spoutId, ZKSERVERS);
	}

	/**
	 * Constructor
	 * @param topic
	 * @param spoutId
	 * @param zkServers
	 */
	public KafkaSpoutFactory(String topic, String spoutId, String[] zkServers) {
		this(topic, spoutId, zkServers, SCHEME);
	}

	/**
	 * 
	 * @param topic
	 * @param spoutId
	 * @param zkServers
	 * @param scheme
	 */
	public KafkaSpoutFactory(String topic, String spoutId, String[] zkServers,
			Scheme scheme) {
		this(topic, spoutId, zkServers, scheme, ZKROOT);
	}

	/**
	 * Constructor
	 * @param topic
	 * @param spoutId
	 * @param zkServers
	 * @param scheme
	 * @param zkRoot
	 */
	public KafkaSpoutFactory(String topic, String spoutId, String[] zkServers,
			Scheme scheme, String zkRoot) {
		this(topic, spoutId, zkServers, scheme, zkRoot, ZKPORT);

	}

	/**
	 * Constructor
	 * @param topic
	 * @param spoutId
	 * @param zkServers
	 * @param scheme
	 * @param zkRoot
	 * @param zkPort
	 */
	public KafkaSpoutFactory(String topic, String spoutId, String[] zkServers,
			Scheme scheme, String zkRoot, Integer zkPort) {
		StringBuffer zkHosts =null;
		
		if (zkServers != null) {
			zkHosts = new StringBuffer();
			for (String s : zkServers) {
				if (zkHosts.length() > 0)
					zkHosts.append(",");
				zkHosts.append(s);
				zkHosts.append(":");
				zkHosts.append(zkPort);
			}
		} else {
			LOG.error("zkServers is null");
			throw new IllegalArgumentException("zkServers not be null");
		}
		
		this.brokerHosts = new ZkHosts(zkHosts.toString());
		this.spoutConf = new SpoutConfig(this.brokerHosts, topic, zkRoot,spoutId);
		this.spoutConf.scheme = new SchemeAsMultiScheme(scheme);
		// 该配置是指，如果该Topology因故障停止处理，下次正常运行时是否从Spout对应数据源Kafka中的该订阅Topic的起始位置开始读取，如果forceFromStart=true，
		// 则之前处理过的Tuple还要重新处理一遍，否则会从上次处理的位置继续处理，保证Kafka中的Topic数据不被重复处理，是在数据源的位置进行状态记录
		spoutConf.forceFromStart = false;
		this.spoutConf.zkServers = Arrays.asList(zkServers);
		this.spoutConf.zkPort = zkPort;
		this.kafkaSpout = new KafkaSpout(this.spoutConf);
	}

	/**
	 * return kafka spout instance
	 * @return
	 */
	public KafkaSpout getKafkaSpout() {
		return kafkaSpout;
	}
}