package com.storm;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;

import com.entity.ClusterResultEntity;
import com.entity.ClusterResultPersistentEntity;
import com.google.gson.Gson;
import com.utility.Conf;
import com.utility.JdbcClient;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ClusterResultPersistentBolt implements IBasicBolt {
	
	private static final Log LOGGER = LogFactory.getLog(ClusterResultPersistentBolt.class);
	private static Conf conf = Conf.getInstance();
	
	private String tableName = conf.getClusterResultTableName();
	private Jedis jedis;
	private JdbcClient jdbcClient;
	Gson gson;
	SimpleDateFormat dateFormat;

	public void prepare(Map map, TopologyContext context) {
		gson = new Gson();
		jedis = new Jedis(conf.getRedisHost(), conf.getRedisPort());
		jdbcClient = new JdbcClient();
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		ClusterResultPersistentEntity persistentEntity = (ClusterResultPersistentEntity) tuple.getValue(0);
		ClusterResultEntity entity = persistentEntity.getClusterResultEntity();
		String entityJsonString = gson.toJson(entity);
		jedis.set(conf.getRedisCurrentClusterResultMap(), gson.toJson(persistentEntity));//当前状态入库redis
		
		String sql = buildSql(persistentEntity, entityJsonString);
		if(entity.getClusterResult().size() > 0) {
			System.out.println(sql);
			try {
				jdbcClient.executeUpdate(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private String buildSql(ClusterResultPersistentEntity persistentEntity,String entityJsonString) {
		return "insert into "+tableName+" set "+
				conf.getClusterResultColGPSTime()+"=\""+dateFormat.format(persistentEntity.getGPSTime())+"\","+
				conf.getClusterResultColCreateTime()+"=\""+dateFormat.format(persistentEntity.getCreateTime())+"\","+
				conf.getClusterResultColClusterResult()+"='"+entityJsonString+"'";
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void cleanup() {
		jedis.close();
		jdbcClient.close();
	}
}
