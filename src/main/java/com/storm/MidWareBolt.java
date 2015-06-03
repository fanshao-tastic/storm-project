package com.storm;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 该bolt用于将收到的tuple直接转发给下游,用于测试
 * @author Dx
 *
 */
public class MidWareBolt implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8833527586092263301L;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("midle"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		collector.emit(new Values(tuple.getString(0)));
	}

	public void prepare(Map map, TopologyContext context) {
	}

}
