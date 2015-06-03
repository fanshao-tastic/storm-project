package com.storm;

import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.utility.Conf;

/**
 * 该类用于配置并封装kafka-Spout
 * @author Dx
 *
 */
public class KafkaStormSpout {
	/**
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	/**
	 * kafkaTopic
	 */
	private String kafkaTopic;
	/**
	 * zookeeper hosts
	 */
	private ZkHosts zkHosts;
	/**
	 * Spout config
	 */
	SpoutConfig spoutConf;
	
	public KafkaStormSpout() {
		kafkaTopic = conf.getTopic();
		zkHosts = new ZkHosts(conf.getZkHosts());
		
		spoutConf = new SpoutConfig(zkHosts, kafkaTopic, "/"+kafkaTopic, UUID.randomUUID().toString());
		spoutConf.zkPort = conf.getZkPort();
		spoutConf.zkServers = conf.getZkServers();
		spoutConf.forceFromStart = conf.isForceFromStart();	//从头开始消费
		spoutConf.socketTimeoutMs = 60 * 1000;
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme()); //定义输出为String类型
	}
	
	public KafkaSpout getKafkaSpout() {
		return new KafkaSpout(spoutConf);
	}
}
