package xue.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import xue.bolt.PrintBolt;
import xue.bolt.WriteBolt;
import xue.spout.PWSpout;

//http://www.cnblogs.com/yourarebest/p/6010959.html
public class PWTopology02 {
	
	public static void main(String[] args) throws  Exception {
		Config cfg = new Config();
		cfg.setNumWorkers(2); //设置使用两个工作者
		cfg.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new PWSpout(),2); //产生两个执行器和俩个任务
		
		//设置bolt的并行度和任务执行器和4个任务
		builder.setBolt("print-bolt", new PrintBolt(),2).shuffleGrouping("spout").setNumTasks(4);
		
		//设置bolt的并行度和任务书：（产生6个执行器和6个任务）
		builder.setBolt("write-bolt", new WriteBolt(),6).shuffleGrouping("print-bolt");
		
		//1.本地模式
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("top2", cfg, builder.createTopology());
//		Thread.sleep(10000);
//		cluster.killTopology("top2");
//		cluster.shutdown();
		
		//2.集群模式
		StormSubmitter.submitTopology("top2", cfg, builder.createTopology());
	}
	
}
