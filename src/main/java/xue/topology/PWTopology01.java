package xue.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import xue.bolt.PrintBolt;
import xue.bolt.WriteBolt;
import xue.spout.PWSpout;


public class PWTopology01 {
	
	public static void main(String[] args) throws Exception {
		Config cfg = new Config();
		cfg.setNumWorkers(2);
		cfg.setDebug(true);
		
		/**
		 * 直线模式 spout -> print-bolt -> write-bolt
		 */
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new PWSpout());
		builder.setBolt("print-bolt", new PrintBolt()).shuffleGrouping("spout");
		builder.setBolt("write-bolt", new WriteBolt()).shuffleGrouping("print-bolt");
		
		//本地模式
		/*LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("top1", cfg, builder.createTopology());
		Thread.sleep(10000);
		cluster.killTopology("top1");
		cluster.shutdown();*/
		
		//集群模式
		StormSubmitter.submitTopology("top1", cfg, builder.createTopology());
	}
}
