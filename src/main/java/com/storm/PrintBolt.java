package com.storm;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 该bolt将tuple直接向stdout输出,可用于测试
 * @author Dx
 *
 */
public class PrintBolt implements IBasicBolt{

	private static final long serialVersionUID = 6622404826397495423L;

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {	
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {
	}

	public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
		System.out.println("+++++++++++++++++++++++++");
		System.out.println(tuple.getString(0));
	}

	public void prepare(Map map, TopologyContext context) {

	}

}
