package com.storm;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.cluster.dbscan.ClusterUtils;
import com.cluster.dbscan.DBSCAN;
import com.entity.ClusterResultEntity;
import com.entity.ClusterResultPersistentEntity;
import com.entity.MessageEntity;
import com.google.gson.Gson;
import com.utility.Conf;
import com.cluster.dbscan.Point;

/**
 * 该Bolt用于从上一个Bolt中传来的Vector取出车辆数据
 * 并将该数据集进行聚类(非分布式聚类)
 * @author Dx
 *
 */
public class EntityClusterBolt implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4202712485219421275L;
	private static final Log LOGGER = LogFactory.getLog(EntityClusterBolt.class);

	private static Conf conf = Conf.getInstance();
	private String outputStringStream = conf.getEntityClusterBoltOutputStreamStr();
	private String outputObjectStream = conf.getEntityClusterBoltOutputStreamObj();
	
	private Gson gson;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStringStream, new Fields(outputStringStream));
		declarer.declareStream(outputObjectStream, new Fields(outputObjectStream));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {
	}

	public void execute(Tuple tuple,BasicOutputCollector collector) {
		System.out.println("====================EntityClusterBolt========================");
		Vector<MessageEntity> entityVector = (Vector<MessageEntity>) tuple.getValue(0);
		DBSCAN dbscan = new DBSCAN(entityVector, 60, 10);//Dbscan聚类
		Vector<Vector<Point>> vector = dbscan.applyDBSCAN();
		ClusterResultEntity entity = ClusterUtils.buildClusterEntity(vector);
		try {
			ClusterResultPersistentEntity persistentEntity = 
					new ClusterResultPersistentEntity(entity, entityVector.get(0).getGPSTime());
			collector.emit(outputStringStream,new Values(gson.toJson(persistentEntity)));
			collector.emit(outputObjectStream, new Values(persistentEntity));
		} catch (Exception e) {
		}
		
//		System.out.println(dbscan.resultList.size());
//		System.out.println(gson.toJson(entity));
//		jedisClient.publish(conf.getRedisChannel(), gson.toJson(entity));
		
//		for(List<Point> list : dbscan.resultList) {
//			System.out.println("第"+(round++)+"簇为:");
//			for(Point p : list) {
//				System.out.println(p.x+","+p.y);
//			}
//		}
//		round = 1;
//		Map<String, MessageEntity> tupleMap =Collections.synchronizedMap((Map<String, MessageEntity>) tuple.getValue(0));
//		Vector<MessageEntity> vector = new Vector<MessageEntity>();
//		for(Entry<String, MessageEntity> entity: tupleMap.entrySet()) {
//			vector.add(entity.getValue());
//		}
//		System.out.println(vector);
//		System.out.println(tupleMap.get("806584099170"));
	}

	public void prepare(Map map, TopologyContext context) {
		gson = new Gson();
		LOGGER.info(Thread.currentThread().getName()+" [EntityClusterBolt] get Redis connection successful!");
	}

}
