package com.utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JDBC链接类,可实例化多个,每个实例代表一个连接
 * @author Dx
 *
 */
public class JdbcClient {
	/**
	 * 日志
	 */
	 private static final Log LOGGER = LogFactory.getLog(JdbcClient.class);
	/**
	 * 全局配置
	 */
	private static Conf conf = Conf.getInstance();
	
	//Connection与特定数据库的连接（会话）。在连接上下文中执行 SQL 语句并返回结果
	private Connection conn;
	//SQL 语句被预编译并存储在 PreparedStatement 对象中。然后可以使用此对象多次高效地执行该语句
	private PreparedStatement statement;
	/*
	 * 加载jdbc driver
	 */
	static {
		try {
			Class.forName(conf.getJdbcDriver());
		} catch (ClassNotFoundException e) {
			throw new ExceptionInInitializerError("Fail to load JDBC driver!");
		}
	}
	
	public JdbcClient() {
		try {
			conn = getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 获取jdbc连接
	 * @return
	 * @throws Exception
	 */
	public Connection getConnection() throws SQLException {
		try {
			//DriverManager管理一组 JDBC 驱动程序的基本服务
			Connection conn = DriverManager.getConnection(conf.getMySqlHost(),conf.getMySqlUser(),conf.getMySqlPassword());
			return conn;
		} catch (SQLException e) {
			LOGGER.error(e.fillInStackTrace());
			throw new SQLException(Thread.currentThread().getName()+" failed to connect to JDBC server!", e);
		}
	}
	
	public PreparedStatement getPreparedStatement(String sql) throws SQLException {
		return conn.prepareStatement(sql);
		//创建一个 PreparedStatement 对象来将参数化的 SQL 语句发送到数据库
	}
	/**
	 * 执行sql查询
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ResultSet executeQuery(String sql) throws SQLException {
		LOGGER.info(Thread.currentThread().getName()+" execute Query:"+sql);
		try {
			statement = conn.prepareStatement(sql);
			return statement.executeQuery();
			//executeQuery:在此 PreparedStatement 对象中执行 SQL 查询，并返回该查询生成的 ResultSet 对象
		} catch (SQLException e) {
			LOGGER.error(e.fillInStackTrace());
			throw new SQLException(Thread.currentThread().getName()+" fail to excute Query:"+sql);
		}
	}
	
	/**
	 * 执行sql插入
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public int executeUpdate(String sql) throws SQLException {
		LOGGER.info(Thread.currentThread().getName()+" execute Update:"+sql);
		try {
			statement = conn.prepareStatement(sql);
			return statement.executeUpdate();
		} catch (Exception e) {
			LOGGER.error(e.fillInStackTrace());
			throw new SQLException(Thread.currentThread().getName()+" fail to excute Update:"+sql);
		}
	}
	
	/**
	 * 关闭数据库链接
	 */
	public void close() {
		try {
			statement.close();
		} catch (SQLException e) {
			LOGGER.error("statement close failed!");
			e.printStackTrace();
		}
		try {
			conn.close();
		} catch (SQLException e) {
			LOGGER.error("connection close failed!");
			e.printStackTrace();
		}
	}
	
	public Connection getReadyConnection() {
		return conn;
	}
	
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}
}
