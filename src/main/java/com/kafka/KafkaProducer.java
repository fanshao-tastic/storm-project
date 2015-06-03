package com.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.utility.Conf;
/**
 * 该类用于对kafka的producer进行封装
 * @author Dx
 *
 */
public class KafkaProducer {

	/**
	 * 全局配置
	 */
	private Conf conf = Conf.getInstance();
	/**
	 * kafka-broker的topic
	 */
	private String topic;
	/**
	 * 定义kafka-producer
	 */
	private Producer<String, String> producer;
	
	public KafkaProducer() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", conf.getKafkaBrokeList());
		topic = conf.getTopic();
		producer = new Producer<String, String>(new ProducerConfig(props));
	}
	
	public void send(String message) {
		producer.send(new KeyedMessage<String, String>(topic, message));
	}

	@Override
	protected void finalize() throws Throwable {
		producer.close();
		super.finalize();
	}
}
