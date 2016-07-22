package com.storm;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;

import com.entity.MessageEntity;
import com.utility.Conf;
import com.utility.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 该Bolt用于接收Tuple,并将其反序列化为对象
 * 在TickTuple未到来时,将转换的对象放在Map中
 * 当TickTuple到来时,将之前的Map转化为Vector发射给下一个Bolt
 * @author Dx
 *
 */
public class CarLocBuildBolt implements IBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 824574675781571237L;
	/**
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	/**
	 * TickTuple发射频率
	 */
	private int emitFrequencyInSeconds = conf.getEmitFrequencyInSeconds();
	/**
	 * 存放<carId,msgEntity>,用于保存车辆信息的时间切片
	 */
	// TODO:考虑在多线程情况下的Map的线程安全问题！
	private Map<String, MessageEntity> currentCarInfoMap = new HashMap<String, MessageEntity>();
	/**
	 * 时间格式化
	 */
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	/**
	 * 发射流的StreamId
	 */
	private String streamId = "MessageEntity";
	/**
	 * 增量计数器
	 */
	private int incrementCount = 0;
	

	public void prepare(Map map, TopologyContext context) {

	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		//设置TickTuple的频率，只需要通过isTickTuple来判断是否为tickTuple, 就可以完成定时触发的功能
	    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
	    return conf;
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if(TupleHelpers.isTickTuple(tuple)) {
//			System.out.println("========isTickTuple========");
			if(incrementCount != 0) {//有新增数据才进行聚类
				incrementCount = 0;//计数器清零
				//用集合封装车辆消息实体对象,传递给下游
				Vector<MessageEntity> vector = new Vector<MessageEntity>();
				for(Entry<String, MessageEntity> entity: currentCarInfoMap.entrySet()) {
					vector.add(entity.getValue());
				}
				collector.emit(new Values(vector));
			}
		} else {
			String[] messages = tuple.getString(0).split(conf.getColumnDelimiter());
			//key:当前车辆的id(唯一),value:当前车况消息实体对象
			currentCarInfoMap.put(messages[conf.getPrimaryKeyIndex()], buildEntity(messages));
			incrementCount++;
		}
	}

	/**
	 * 从字符串数组构造消息实体对象
	 * @param messages
	 * @return
	 */
	private MessageEntity buildEntity(String[] messages) {
		try {
			int ID = Integer.parseInt(messages[0]);
			String CompanyID = messages[1];
			String VehicleSimID = messages[2];
			Date GPSTime = dateFormat.parse(messages[3]);
			double GPSLongitude = Double.parseDouble(messages[4]);
			double GPSLatitude = Double.parseDouble(messages[5]);
			int GPSSpeed = Integer.parseInt(messages[6]);
			int GPSDirection = Integer.parseInt(messages[7]);
			int PassengerState = Integer.parseInt(messages[8]);
			int ReadFlag = Integer.parseInt(messages[9]);
			Date CreateDate = dateFormat.parse(messages[10]);
			MessageEntity entity = new MessageEntity(
					ID, 
					CompanyID, 
					VehicleSimID, 
					GPSTime, 
					GPSLongitude, 
					GPSLatitude,
					GPSSpeed,
					GPSDirection,
					PassengerState,
					ReadFlag,
					CreateDate);
			return entity;
		} catch (ParseException e) {
		} catch (NumberFormatException e) {
		}
		return null;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MessageEntity"));
	}

	public void cleanup() {
	}
}
