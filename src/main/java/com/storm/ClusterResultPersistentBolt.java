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

/**
 * 该类用于将聚类后的结果进行持久化
 * 1)将当前结果放入redis(覆盖)
 * 2)将当前结果放入持久化数据库(插入)[当前版本:mysql]
 * @author Dx
 *
 */
public class ClusterResultPersistentBolt implements IBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1894278991684178637L;
	/**
	 * log
	 */
	private static final Log LOGGER = LogFactory.getLog(ClusterResultPersistentBolt.class);
	/**
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	/**
	 * 待持久化的表名
	 */
	private String tableName = conf.getClusterResultTableName();
	/**
	 * jedis客户端
	 */
	private Jedis jedis;
	/**
	 * 持久化数据库jdbc客户端
	 */
	private JdbcClient jdbcClient;
	/**
	 * json格式化工具
	 */
	private Gson gson;
	/**
	 * 日期格式化工具
	 */
	private SimpleDateFormat dateFormat;

	public void prepare(Map map, TopologyContext context) {
		gson = new Gson();
		jedis = new Jedis(conf.getRedisHost(), conf.getRedisPort());
		LOGGER.info(Thread.currentThread().getName()+" [ClusterResultPersistentBolt] get Redis connection successful!");
		jdbcClient = new JdbcClient();
		LOGGER.info(Thread.currentThread().getName()+" [ClusterResultPersistentBolt] get JDBC connection successful!");
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		//取出聚类的持久化结果
		ClusterResultPersistentEntity persistentEntity = (ClusterResultPersistentEntity) tuple.getValue(0);
		//取出聚类结果
		ClusterResultEntity entity = persistentEntity.getClusterResultEntity();
		String entityJsonString = gson.toJson(entity);
		//当前状态入库redis
		jedis.set(conf.getRedisCurrentClusterResultMap(), gson.toJson(persistentEntity));
		//插入持久化数据库
		String sql = buildSql(persistentEntity, entityJsonString);
		if(entity.getClusterResult().size() > 0) {
			//System.out.println(sql);
			try {
				jdbcClient.executeUpdate(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 拼接插入持久化数据的sql语句
	 * @param persistentEntity
	 * @param entityJsonString
	 * @return
	 */
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
//		jdbcClient.close();
	}
}
