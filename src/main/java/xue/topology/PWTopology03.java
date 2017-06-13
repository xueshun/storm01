package xue.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import xue.bolt.PrintBolt;
import xue.bolt.WriteBolt;
import xue.spout.PWSpout;

/**
 * Storm流分组
 *  shuffle Grouping随机分组 保证每个bolt接收到的tuple数目是相同的
 *  Fields Grouping按字段分组：比如按userid来分组，具有相同的userid的tuple会被分到相同的Bolts
 *  	而不同的userid则会被分配到不同的Bolts
 *  All Grouping广播发送：对每个tuple，所有的Bolts都会接受到
 *  Global Grouping全局分组： 这个tuple被分配到storm中的一个bolt的中一个task。在具体一点就是分配到
 *  	id值最低的那个task
 *  Non Grouping无分组： 假设你不关心六十如何分组的可以使用这种方式，目前这种分组和随机分组是一样的效果，不同的是Storm
 *  	会把这个Bolt放到Bolt的订阅者的同一个线程中执行
 *  Direct Grouping 直接分组：这种分组以为着消息的发送者指定有消息接收的那个Task处理这个消息。只有被声明为Direct
 *  	Stream的消息流可以声明这种分组方法。TopolopyContext来获取处理他的消息的taskid（OutoutCollector.emit）
 *  	方法也会返回taskid
 *  本地分组：如果目标bolt在同一工作进程存在一个或多个任务，元组会随机分配给执行任务，否则该分组方式与随机分组方式时一样的
 * @author Administrator
 *
 */
public class PWTopology03 {
	public static void main(String[] args) throws Exception {
		
		Config cfg = new Config();
		cfg.setNumWorkers(2);
		cfg.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new PWSpout(),4);
		builder.setBolt("print-bolt", new PrintBolt(),4).shuffleGrouping("spout");
		
		//随机分组
		builder.setBolt("write-bolt", new WriteBolt(),4).shuffleGrouping("print-bolt");
		//设置字段分组
		//builder.setBolt("write-bolt", new WriteBolt(),10).fieldsGrouping("print-bolt", new Fields("write"));
		//设置广播分组
		//builder.setBolt("write-bolt", new WriteBolt(),4).allGrouping("print-bolt");
		//设置全局分组
		//builder.setBolt("write-bolt", new WriteBolt(),4).globalGrouping("print-bolt");
		
		//本地模式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("top3", cfg, builder.createTopology());
		Thread.sleep(10000);
		cluster.killTopology("top3");
		cluster.shutdown();
	}
}
