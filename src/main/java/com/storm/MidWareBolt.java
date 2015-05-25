package com.storm;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MidWareBolt implements IBasicBolt {

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("midle"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		collector.emit(new Values(tuple.getString(0)));
	}

	public void prepare(Map map, TopologyContext context) {
		// TODO Auto-generated method stub
	}

}
