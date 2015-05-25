package com.storm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.utility.Conf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Strom Topology Main
 *
 */
public class Main {
	private static final Log LOG = LogFactory.getLog(Main.class);

	public static void main(String[] args) throws AlreadyAliveException, 
	InvalidTopologyException, InterruptedException {
		//全局配置
		Conf conf = Conf.getInstance();
		
		// 构建Topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaStormSpout().getKafkaSpout(), 1);
//		builder.setBolt("PrintBolt", new PrintBolt(), 1).shuffleGrouping("spout");
		builder.setBolt("RedisPubBolt", new RedisPubBolt(conf.getRedisCarLocChannel()),1).shuffleGrouping("spout");
		builder.setBolt("CurrentCarLocPersistentBolt", new CurrentCarLocPersistentBolt(),1).shuffleGrouping("spout");
		
//		builder.setBolt("MidWareBolt", new MidWareBolt(),1).shuffleGrouping("spout");
//		builder.setBolt("CarLocBuildBolt", new CarLocBuildBolt(),1).shuffleGrouping("spout");
//		builder.setBolt("EntityClusterBolt", new EntityClusterBolt(),1).shuffleGrouping("CarLocBuildBolt");
//		builder.setBolt("RedisPubBolt2", new RedisPubBolt(conf.getRedisCarClusterChannel()),1).shuffleGrouping("EntityClusterBolt", conf.getEntityClusterBoltOutputStreamStr());
//		builder.setBolt("ClusterResultPersistentBolt", new ClusterResultPersistentBolt(),1).shuffleGrouping("EntityClusterBolt", conf.getEntityClusterBoltOutputStreamObj());

		Config config = new Config();
		config.setDebug(false); // 启用or关闭 调试开关

		if (args != null && args.length > 0) {
			config.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], config,builder.createTopology());
		} else {
			config.setMaxTaskParallelism(3);//设置bolt的最大并行度

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("topologyDebug", config, builder.createTopology());

			Thread.sleep(60 * 1000);	//等待后关闭调试Topology

			cluster.shutdown();
		}
	}
}
