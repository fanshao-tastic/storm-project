package com.utility;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * 该类仅用于测试Storm的Pub端的功能
 * @author Dx
 *
 */
public class PubSubServer extends JedisPubSub{

	@Override
	public void onMessage(String channel, String message) {
//		super.onMessage(channel, message);
		System.out.println("onMessage:channel["+channel+"],message["+message+"]");
	}

	@Override
	public void onSubscribe(String channel, int subscribedChannels) {
//		super.onSubscribe(channel, subscribedChannels);
		System.out.println("onSubscribe:channel["+channel+"],subscribedChannels["+subscribedChannels+"]");
	}
}
