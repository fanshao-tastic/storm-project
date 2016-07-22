package com.storm;

import com.utility.Conf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Strom Topology Main
 *
 */
public class Main {
	
	public static void main(String[] args) throws InterruptedException, 
	AlreadyAliveException, InvalidTopologyException,AuthorizationException {
		//全局配置
		Conf conf = Conf.getInstance();
		
		// 构建Topology
		TopologyBuilder builder = new TopologyBuilder();
		//添加kafka-spout
		builder.setSpout("spout", new KafkaStormSpout().getKafkaSpout(), conf.getSpoutNum());
		//打印测试bolt
//		builder.setBolt("PrintBolt", new PrintBolt(), 1).shuffleGrouping("spout");
		//用于推送当前车况的bolt
//		builder.setBolt("RedisPubBolt", new RedisPubBolt(conf.getRedisCarLocChannel()),conf.getRedisPubBolt1Num())
//			   .shuffleGrouping("spout");
		//用于持久化当前车况的bolt
		builder.setBolt("CurrentCarLocPersistentBolt", new CurrentCarLocPersistentBolt(),conf.getCurrentCarLocPersistentBoltNum())
			   .shuffleGrouping("spout");
		//中间层测试bolt
	//	builder.setBolt("MidWareBolt", new MidWareBolt(),1).shuffleGrouping("spout");
		//从当前spout中接收消息并转化为对象(需要用到TickTuple,做定时)
//		builder.setBolt("CarLocBuildBolt", new CarLocBuildBolt(),conf.getCarLocBuildBoltNum())
//			   .shuffleGrouping("spout");
//		//聚类bolt
//		builder.setBolt("EntityClusterBolt", new EntityClusterBolt(),conf.getEntityClusterBoltNum())
//			   .shuffleGrouping("CarLocBuildBolt");
//		//用于推送聚类结果的bolt
//		builder.setBolt("RedisPubBolt2", new RedisPubBolt(conf.getRedisCarClusterChannel()),conf.getRedisPubBolt2Num())
//			   .shuffleGrouping("EntityClusterBolt", conf.getEntityClusterBoltOutputStreamStr());
//		//用于持久化聚类结果的bolt
//		builder.setBolt("ClusterResultPersistentBolt", new ClusterResultPersistentBolt(),conf.getClusterResultPersistentBoltNum())
//			   .shuffleGrouping("EntityClusterBolt", conf.getEntityClusterBoltOutputStreamObj());

		Config config = new Config();
		config.setDebug(false); // 启用or关闭 调试开关

		if (args != null && args.length > 0) {//提交到集群运行
			config.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], config,builder.createTopology());
		} else {//本地调试运行
			config.setMaxTaskParallelism(3);//设置bolt的最大并行度

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("topologyDebug", config, builder.createTopology());

			Thread.sleep(conf.getDebugRunTime() * 1000);	//等待后关闭调试Topology

			cluster.shutdown();
		}
	}
}
