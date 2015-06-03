package com.storm;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.utility.Conf;


/**
 * 用于将Tuple输出到Redis的Publish端
 * @author Dx
 *
 */
public class RedisPubBolt implements IBasicBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2662741522976613854L;
	/**
	 * 日志
	 */
	private static final Log LOGGER = LogFactory.getLog(RedisPubBolt.class);
	/**
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	/**
	 * Redis客户端
	 */
	private Jedis jedisClient;
	/**
	 * 该redis发布消息的channel名称
	 */
	private String redisChannel;
	
	public RedisPubBolt(String redisChannel) {
		this.redisChannel = redisChannel;
	}
	
	public void prepare(Map map, TopologyContext context) {
		jedisClient = new Jedis(conf.getRedisHost(), conf.getRedisPort());
		LOGGER.info(Thread.currentThread().getName()+" [RedisPubBolt] get Redis connection successful!");
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	/**
	 * 将tuple中的内容写入redis的channel的pub端
	 */
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		jedisClient.publish(redisChannel, tuple.getString(0));
	}

	public void cleanup() {
		jedisClient.close();
	}
	
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
}
