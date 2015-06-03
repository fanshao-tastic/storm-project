package com.kafka;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.utility.Conf;
import com.utility.JdbcClient;

public class MysqlProducer {

	/**
	 * log
	 */
	private static final Log LOGGER = LogFactory.getLog(MysqlProducer.class);
	/**
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	/**
	 * kafkaProducer
	 */
	private KafkaProducer kafkaProducer = new KafkaProducer();
	/**
	 * 待导入的表名
	 */
	private String tableName;
	/**
	 * JdbcClient
	 */
	private JdbcClient jdbcClient = new JdbcClient();
	/**
	 * 一次取出的数据量
	 */
	private int batchBlock = conf.getBatchBlock();
	/**
	 * 设置是否有order by 命令标志位
	 */
	private String orderBy = null;
	
	public MysqlProducer(String tableName) {
		this.tableName = tableName;
	}
	/**
	 * 执行入口函数
	 */
	public void execute() {
		int columnNums = getColumnNums();
		int rowNums = getRowNums();
		int i=0;
		String sql;
		for(;i<rowNums-batchBlock;i+=batchBlock) {
			int begin = i;
			int end = i+batchBlock;
			if(orderBy == null) {
				sql = "select * from "+tableName+" limit "+begin+","+batchBlock;
			} else {
				sql = "select * from "+tableName+" order by "+orderBy+" limit "+begin+","+batchBlock;
			}
			executeQueryBatch(sql, columnNums);
			LOGGER.debug(Thread.currentThread().getName()+" has sent "+i+" messages");
			//两次batch直接间隔时间,配合batchBlock的大小,用于模拟实时的效果
			try {
				Thread.sleep(conf.getSleepMilliSecond());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		sql = "select * from "+tableName+" limit "+i+","+batchBlock;
		executeQueryBatch(sql, columnNums);
		LOGGER.info(Thread.currentThread().getName()+" has sent "+rowNums+" messages");
	}
	/**
	 * 获取表格列数
	 * @return 表的列数
	 */
	private int getColumnNums() {
		String sql = "select * from "+tableName+" limit 1";
		int columnNums = 0;
		try {
			ResultSet resultSet = jdbcClient.executeQuery(sql);
			columnNums = resultSet.getMetaData().getColumnCount();
			LOGGER.info("The table "+tableName+" has "+columnNums+" columnNums");
		} catch (SQLException e) {
			LOGGER.error("execute "+sql+" error!");
			e.printStackTrace();
		}
		return columnNums;
	}
	
	/**
	 * 获取全部行数
	 * @return 表的行数
	 */
	private int getRowNums() {
		String sql = "select count(*) from "+tableName;
		int rowNums = 0;
		try {
			ResultSet resultSet = jdbcClient.executeQuery(sql);
			resultSet.next();
			rowNums = resultSet.getInt(1);
			LOGGER.info("The table "+tableName+" has "+rowNums+" rowNums");
		} catch (SQLException e) {
			LOGGER.error("execute "+sql+" error!");
			e.printStackTrace();
		}
		return rowNums;
	}
	/**
	 * 分页取出所有行
	 * @param sql
	 * @param columnNums
	 */
	private void executeQueryBatch(String sql,int columnNums) {
		try {
			ResultSet resultSet = jdbcClient.executeQuery(sql);
			StringBuffer strBuffer = new StringBuffer();
			while (resultSet.next()) {
				int i=1;
	        	for(;i<columnNums;i++) {
	        		strBuffer.append(resultSet.getString(i)).append(",");
	        	}
	        	strBuffer.append(resultSet.getString(i));
	        	//打印测试
	        	//System.out.println(strBuffer);
	        	kafkaProducer.send(strBuffer.toString());
	        	strBuffer.delete(0, strBuffer.length());
			}
		} catch (SQLException e) {
			LOGGER.error("execute batchQuery "+sql+" error!");
			e.printStackTrace();
		}
	}

	/**
	 * 设置一次查询记录的条数
	 * @param batchBlock
	 */
	public void setBatchBlock(int batchBlock) {
		this.batchBlock = batchBlock;
	}
	/**
	 * 设置是否有排序
	 * @param orderBy
	 */
	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}
}
