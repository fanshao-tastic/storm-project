package com.storm;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;

import com.utility.Conf;
import com.utility.JdbcClient;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 用于将当前收到的最新车辆信息更新到Redis和Mysql中
 * @author Dx
 *
 */
public class CurrentCarLocPersistentBolt implements IBasicBolt {
	
	/*
	 * 日志
	 */
	private static final Log LOGGER = LogFactory.getLog(CurrentCarLocPersistentBolt.class);
	/*
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	/*
	 * Redis配置
	 */
	private Jedis jedisClient;
	private String redisHost = conf.getRedisHost();
	private int redisPort = conf.getRedisPort();
	/*
	 * 持久化数据库配置
	 */
	private JdbcClient jdbcClient;
	private PreparedStatement statement;
	private int batchSize = conf.getBatchSize();//每隔conf.getBatchSize(),进行一次批量插入
	private int currentSize = 0;
	private int roundCount = 0;

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
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

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String tupleString = tuple.getString(0);
		String[] message = tupleString.split(conf.getColumnDelimiter());
		String carId = message[conf.getPrimaryKeyIndex()];
		//更新redis表
		jedisClient.hset(conf.getRedisCurrentCarLocMap(),carId,tupleString);
		//batch方式持久化到数据库
		insertBatchToDB(message);
		//insertToDB(message);
	}

	/**
	 * 批量导入方式
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
				System.out.println("executeBatch"+" "+roundCount++);
			} catch (SQLException e) {
			}
			currentSize = 0; 
		}
	}

	/**
	 * 单条导入方式
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

	public void prepare(Map map, TopologyContext context) {
		jedisClient = new Jedis(redisHost, redisPort);
		jdbcClient = new JdbcClient();
		String preStatementSql = "insert into "+conf.getCarGPSLogTableName()+" set "
				+ "ID=?,CompanyID=?,VehicleSimID=?,GPSTime=?,GPSLongitude=?,GPSLatitude=?,"
				+ "GPSSpeed=?,GPSDirection=?,PassengerState=?,ReadFlag=?,CreateDate=?";
		try {
			statement = jdbcClient.getPreparedStatement(preStatementSql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		LOGGER.info(Thread.currentThread().getName()+" [RedisCurrentCarLocBolt] get Redis connection successful!");
	}

}
