package xue.spout;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * storm 数据源
 * @author Administrator
 *
 */

public class PWSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;

	//数据
	private static final Map<Integer, String> map = new HashMap<Integer,String>();
	
	static{
		map.put(0, "java");
		map.put(1, "php");
		map.put(2, "groovy");
		map.put(3, "python");
		map.put(4, "ruby");
	}
	
	@Override
	public void open(Map conf, TopologyContext contxt, SpoutOutputCollector collector) {
		//对spout进行初始化
		this.collector = collector;
	}
	
	/**
	 * 轮询 tuple
	 */
	@Override
	public void nextTuple() {
		//随机发送一个单词
		final Random r = new Random();
		int num = r.nextInt(5);
		try {
			Thread.sleep(500);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.collector.emit(new Values(map.get(num)));
	}

	
	/**
	 * 声明发送手机的field
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//进行声明
		declarer.declare(new Fields("print"));
	}
	
}
