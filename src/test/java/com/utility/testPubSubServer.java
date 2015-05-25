package com.utility;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class testPubSubServer {

	private static Conf conf = Conf.getInstance();
	private Jedis jedisClient;
	private PubSubServer pubSubClient = new PubSubServer();
	
	@Before
	public void setUp() throws Exception {
		jedisClient = new Jedis(conf.getRedisHost(), conf.getRedisPort(),0);
	}

	@Test
	public void testOnMessageStringString() {
		pubSubClient.proceed(jedisClient.getClient(), conf.getRedisCarClusterChannel());
	}

//	@Test
	public void testOnSubscribeStringInt() {
		fail("Not yet implemented");
	}

}
