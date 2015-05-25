package com.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.utility.Conf;

public class KafkaProducer {

	private Conf conf = Conf.getInstance();
	
	private String topic;
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
