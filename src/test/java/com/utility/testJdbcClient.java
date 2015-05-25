package com.utility;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

public class testJdbcClient {
	
	JdbcClient client = new JdbcClient();

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testExcuteUpdate() {
		String sql = "insert into ClusterResult set GPSTime='2015-01-01 09:11:12',CreateTime='2015-01-01 09:11:13',ClusterResult='adf'";
		try {
			System.out.println(client.executeUpdate(sql));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
