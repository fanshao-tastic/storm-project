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
import com.cluster.dbscan.Point;
import com.entity.ClusterResultEntity;
import com.entity.ClusterResultPersistentEntity;
import com.entity.MessageEntity;
import com.google.gson.Gson;
import com.utility.Conf;

/**
 * 该Bolt用于从上一个Bolt中传来的Vector取出车辆数据
 * 并将该数据集进行聚类(当前版本:非分布式聚类)
 * 该类会发射两条流:
 * 1)字符串型,直接将persistentEntity转换为json字符串,用于交给下游的RedisPubBolt进行推送
 * 2)persistentEntity对象类型
 * @author Dx
 *
 */
public class EntityClusterBolt implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4202712485219421275L;
	/**
	 * log
	 */
	private static final Log LOGGER = LogFactory.getLog(EntityClusterBolt.class);
	/**
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	/**
	 * 定义string类型的流的名称
	 */
	private String outputStringStream = conf.getEntityClusterBoltOutputStreamStr();
	/**
	 * 定义obj类型的流的名称
	 */
	private String outputObjectStream = conf.getEntityClusterBoltOutputStreamObj();
	
	private Gson gson;

	public void prepare(Map map, TopologyContext context) {
		gson = new Gson();
		LOGGER.info(Thread.currentThread().getName()+" [EntityClusterBolt] get Redis connection successful!");
	}
	
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void execute(Tuple tuple,BasicOutputCollector collector) {
		Vector<MessageEntity> entityVector = (Vector<MessageEntity>) tuple.getValue(0);
		//Dbscan聚类
		DBSCAN dbscan = new DBSCAN(entityVector, conf.getEps(), conf.getMinPts());
		Vector<Vector<Point>> vector = dbscan.applyDBSCAN();
		//从聚类结果构造聚类结果实体对象
		ClusterResultEntity entity = ClusterUtils.buildClusterEntity(vector);
		try {
			ClusterResultPersistentEntity persistentEntity = 
					new ClusterResultPersistentEntity(entity, entityVector.get(0).getGPSTime());
			collector.emit(outputStringStream,new Values(gson.toJson(persistentEntity)));
			collector.emit(outputObjectStream, new Values(persistentEntity));
		} catch (Exception e) {
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStringStream, new Fields(outputStringStream));
		declarer.declareStream(outputObjectStream, new Fields(outputObjectStream));
	}
	
	public void cleanup() {
	}
}
