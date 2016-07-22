package com.storm;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PreFilterBolt implements IBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5202712485219421271L;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
}
