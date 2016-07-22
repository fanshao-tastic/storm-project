package com.storm;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.junit.Before;
import org.junit.Test;

import com.utility.JdbcClient;

public class testJdbcClient {
	
	JdbcClient client = new JdbcClient();

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testExecuteQuery() {
		
		String sql = "select * from SourceDate limit 1";
		SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ResultSet resultSet;
        try {
			resultSet = client.executeQuery(sql);
			int cloumnNums = resultSet.getMetaData().getColumnCount();
			sql = "select * from SourceDate limit 2";
			StringBuffer strBuffer = new StringBuffer();
			resultSet = client.executeQuery(sql);
			while(resultSet.next()) {
				int i=1;
	        	for(;i<cloumnNums;i++) {
	        		strBuffer.append(resultSet.getString(i)).append(",");
	        	}
	        	strBuffer.append(resultSet.getString(i));
	        	System.out.println(strBuffer);
	        	strBuffer.delete(0, strBuffer.length());
	        }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQueryCount() {
		System.out.println("==============");
		String sql = "select count(*) from SourceDate";
		try {
			ResultSet resultSet = client.executeQuery(sql);
			resultSet.next();
			int rownums = resultSet.getInt(1);
			System.out.println(rownums);
			int block = 100000;//查询的分页大小
			sql = "select * from SourceDate limit 1";
			resultSet = client.executeQuery(sql);
			int cloumnNums = resultSet.getMetaData().getColumnCount();
			System.out.println(cloumnNums);
//			for(int i=0;i<rownums;i+=block) {
//				sql = "select * from from VehicleData20100901 limit ";
//			}
//			System.out.println(resultSet.getInt(1));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
