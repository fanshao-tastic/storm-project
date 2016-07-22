package com.storm;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;

import com.cluster.dbscan.CoordTransform;
import com.cluster.dbscan.Point;
import com.utility.Conf;
import com.utility.JdbcClient;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 用于将当前收到的最新车辆信息更新到Redis和持久化到Mysql中
 * @author Dx
 *
 */
public class CurrentCarLocPersistentBolt implements IBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7442352964942376281L;
	/**
	 * 日志
	 */
	private static final Log LOGGER = LogFactory.getLog(CurrentCarLocPersistentBolt.class);
	/**
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	/**
	 * Redis配置
	 */
	private Jedis jedisClient;
	/*
	 * 持久化数据库配置
	 */
	private JdbcClient jdbcClient;
	private PreparedStatement statement;
	
	/**
	 * 定义一次持久化的batch的大小
	 */
	private int batchSize = conf.getBatchSize();//每隔conf.getBatchSize(),进行一次批量插入
	/**
	 * 当前batch的大小
	 */
	private int currentSize = 0;
	
	public void prepare(Map map, TopologyContext context) {
		jedisClient = new Jedis(conf.getRedisHost(), conf.getRedisPort());
		LOGGER.info(Thread.currentThread().getName()+" [RedisCurrentCarLocBolt] get Redis connection successful!");
		jdbcClient = new JdbcClient();
		//预定义插入操作的sql语句格式
		String preStatementSql = "insert into "+conf.getCarGPSLogTableName()+" set "
				+ "ID=?,CompanyID=?,VehicleSimID=?,GPSTime=?,GPSLongitude=?,GPSLatitude=?,"
				+ "GPSSpeed=?,GPSDirection=?,PassengerState=?,ReadFlag=?,CreateDate=?";
		try {
			statement = jdbcClient.getPreparedStatement(preStatementSql);
		} catch (SQLException e) {
			LOGGER.error(Thread.currentThread().getName()+" statement error!");
			e.printStackTrace();
		}
		LOGGER.info(Thread.currentThread().getName()+" [RedisCurrentCarLocBolt] get JDBC connection successful!");
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String tupleString = tuple.getString(0);
		String[] message = tupleString.split(conf.getColumnDelimiter());
		String carId = message[conf.getPrimaryKeyIndex()];
		double lng=Double.parseDouble(message[conf.getPrimaryLngIndex()]);
		double lat=Double.parseDouble(message[conf.getPrimaryLatIndex()]);
		//更新redis表
	//	jedisClient.hset(conf.getRedisCurrentCarLocMap(),carId,tupleString);
		
		//坐标转换
		CoordTransform coordTransform=new CoordTransform();
		Point point=null;
		point=coordTransform.wgs84tobd09(lng,lat);
		if(point!=null){
		message[conf.getPrimaryLngIndex()]=Double.toString(point.x);
		message[conf.getPrimaryLatIndex()]=Double.toString(point.y);
		
		//batch方式持久化到数据库
		insertBatchToDB(message);
		//insertToDB(message);
		}else{
			;
		}
	}

	/**
	 * 批量插入方式
	 * 从tuple字符串中构建插入表中的sql语句(和表结构相关!)
	 * @param message
	 */
	private void insertBatchToDB(String[] message) {
		if(currentSize < batchSize-1) {
			try {
				for(int i=0;i<message.length;i++) {
					statement.setString(i+1, message[i]);
				}
				statement.addBatch();
			} catch (SQLException e) {
			}
			currentSize++;
		} else {
			try {
				for(int i=0;i<message.length;i++) {
					statement.setString(i+1, message[i]);
				}
				statement.addBatch();
				statement.executeBatch();
			} catch (SQLException e) {
			}
			currentSize = 0; 
		}
	}

	/**
	 * 单条插入方式(不推荐)
	 * 从tuple字符串中构建插入表中的sql语句(和表结构相关!)
	 * @param message
	 * @return
	 */
	private void insertToDB(String[] message) {
		try {
			String sql = "insert into "+conf.getCarGPSLogTableName()+" set "+
					"ID="+message[0]+",CompanyID="+message[1]+
					",VehicleSimID="+message[2]+",GPSTime=\""+message[3]+"\""+
					",GPSLongitude="+message[4]+",GPSLatitude="+message[5]+
					",GPSSpeed="+message[6]+",GPSDirection="+message[7]+
					",PassengerState="+message[8]+",ReadFlag="+message[9]+
					",CreateDate=\""+message[10]+"\"";
			jdbcClient.executeUpdate(sql);
		} catch (Exception e) {
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
	
	public void cleanup() {
		jedisClient.close();
		try {
			statement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
//		jdbcClient.close();
	}
}
