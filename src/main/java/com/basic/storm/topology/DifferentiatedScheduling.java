package com.basic.storm.topology;

import com.basic.util.SynopsisHashMap;
import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.math.BigInteger;
import java.sql.Connection;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is a basic example of a Storm topology.
 */
public class DifferentiatedScheduling {

	public static class Constraints {
        public static int Threshold_r = 6;
	    public static int Threshold_l = 16;
		public static double Threshold_p = 0.01;

		public static int COUNTER_NUMBER = 8;   //number of machines
		public static int KEYS = 1000000 / (COUNTER_NUMBER / 8);  //number of tuples in each machine
		public static int AGGREGATE_INTERVAL = 120; //aggreatiom interval
}
	
	public static class WordGenerator extends BaseRichSpout {
		private static final long serialVersionUID = 1L;
		private SpoutOutputCollector collector;   //default
		private FileReader file = null;         //filereader
		private BufferedReader reader = null;   //bufferreader
		private FileWriter fileWriter;
		private ConcurrentHashMap<UUID, Long> pending;

		@Override
		public void open(Map conf, TopologyContext context,
						 SpoutOutputCollector collector) {

			try {
				file = new FileReader("/root/wind/output/out20.txt");
				reader = new BufferedReader(file);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			try {
				this.fileWriter = new FileWriter("/root/wind/output/latency_ds_040301.txt");
			} catch (IOException e) {
				throw new RuntimeException("Error write file");
			}
			this.collector = collector;
			this. pending = new ConcurrentHashMap<UUID, Long>();
		}

		@Override
		public void nextTuple() {
			String word=null;
			try {
				word = reader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (word == null)
				return;
			UUID msgId = UUID.randomUUID();
			this.collector.emit(new Values(word.toString()),msgId);    //read a line, emit as a word
			Long t1 = System.nanoTime();
			this.pending.put(msgId,t1);
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

		@Override
		public void ack(Object msgId) {
			Long t2= System.nanoTime();
			t2 = t2-this.pending.get(msgId);
			this.pending.remove(msgId);
			t2 = t2/1000000;
			try{
				fileWriter.write(t2 + "\r\n");
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		@Override
		public void fail(Object msgId) {
		}
		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
	}

	public static class SplitterBolt extends BaseRichBolt {
		private static final long serialVersionUID = -4094707939635564788L;
		private FileWriter fileWriter;
		private OutputCollector collector;
		private CountingBloomFilter bf;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			try {
				this.fileWriter = new FileWriter("/root/wind/output/040302splittest.txt");
			} catch (IOException e) {
				throw new RuntimeException("Error write file");
			}
			this.collector = collector;
			this.bf = new CountingBloomFilter(16,4,1);
        }

        public void execute(Tuple tuple) {
			if(tuple.getSourceComponent().equals("start")){
				String word = tuple.getStringByField("word");
				collector.emit("coin", new Values(word));
				Key ky = new Key(word.getBytes());
				if(bf.membershipTest(ky))
					collector.emit("hot", tuple, new Values(word));
				else
					collector.emit("nothot", tuple, new Values(word));

			}else {
				String key = tuple.getStringByField("word");
				Integer type = tuple.getIntegerByField("type");
				Key hk = new Key(key.getBytes());
				if(!bf.membershipTest(hk) && type.equals(1))
					bf.add(hk);
				if(bf.membershipTest(hk) && type.equals(0))
					bf.delete(hk);
			}
			collector.ack(tuple);
        }

        public void cleanup(){     	
        }
        
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream("coin", new Fields("word"));
			declarer.declareStream("hot", new Fields("word"));
			declarer.declareStream("nothot", new Fields("word"));
        }
        
		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
    }

    public static class CoinBolt extends BaseRichBolt  {
    	private OutputCollector collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
        }

        public void execute(Tuple tuple) {
        	String word = tuple.getStringByField("word");
			Integer count = - Constraints.Threshold_r;
			int rand = (int)(Math.random()*2);
			while(rand == 0 && count < Constraints.Threshold_l-1)     //Max length set equal to max length+r;
			{
				rand = (int)(Math.random()*2);
				count++;
			}
			if(count >= 0)
				collector.emit(new Values(word,count));
			collector.ack(tuple);
        }

        public void cleanup(){
        }
        
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
        }
        
		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
    }

    public static class PredictorBolt extends BaseRichBolt {
		private OutputCollector collector;
		private FileWriter fileWriter;
		private SynopsisHashMap<String, BitSet> counts = new SynopsisHashMap<String, BitSet>();

		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			try {
				this.fileWriter = new FileWriter("/root/wind/output/040302synopsismaxtest.txt");
			} catch (IOException e) {
				throw new RuntimeException("Error write file");
			}
			this.collector = collector;
		}

		public void execute(Tuple tuple) {
			String word = tuple.getStringByField("word");
			Integer count = tuple.getIntegerByField("count");
			BitSet bitmap = new BitSet(Constraints.Threshold_l);
			if(counts.get(word)!=null)
				bitmap = (BitSet) counts.get(word).clone();

			bitmap.set(count);
			counts.put(word,bitmap);

			if(bitmap.cardinality() >= 2)
				collector.emit(new Values(word,1));
			try{
				fileWriter.write(word + "  " + bitmap + "\r\n");
			}catch(IOException e){
				e.printStackTrace();
			}
			int cap = counts.capacity();
			int si = counts.size();
			if(cap>512){
				try{
					fileWriter.write(cap + "  " + si + "\r\n");
				}catch(IOException e){
					e.printStackTrace();
				}
			}
			for(int i = 0; i<cap*Constraints.Threshold_p;i++){
				String nohot = counts.getrandomkey();
				if(nohot != null){
					BitSet bitm = new BitSet(Constraints.Threshold_l);
					bitm = (BitSet) counts.get(nohot).clone();
					long[] lo = bitm.toLongArray();
					if(lo.length > 0){
						for(int j=0;j<lo.length - 1;j++){
							lo[j] = lo[j] >>> 1;
							lo[j] = lo[j] | (lo[j+1] << 63);
						}
						lo[lo.length-1] = lo[lo.length-1] >>> 1;
					}
					bitm = BitSet.valueOf(lo);
					if(bitm.isEmpty())
					{
						counts.remove(nohot);
						collector.emit(new Values(nohot,0));
					}
					else
						counts.put(nohot,bitm);
				}
			}
			collector.ack(tuple);
		}

		public void cleanup(){
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word","type"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
    }


    public static class CounterBolt extends BaseRichBolt {
		private static final long serialVersionUID = -2350373680379322599L;
		private Map<String, Integer> counts = new HashMap<String, Integer>();
		private static final int DEFAULT_TICK_FREQUENCY_SECONDS = 1;
		private int tickCount = 0;
		OutputCollector collector;

		@Override
		public void execute(Tuple tuple) {

			if (isTickTuple(tuple)) {
				tickCount++;
				if(tickCount == Constraints.AGGREGATE_INTERVAL){
					emit();
					counts.clear();
					tickCount = 0;
				}
			} else {
				tryWait(1000000);
				String word = tuple.getStringByField("word");
				if (!word.isEmpty()) {
					Integer count = counts.get(word);
					if (count == null) {
						count = 0;
					}
					count++;
					counts.put(word, count);
				}
				collector.ack(tuple);
			}
		}

		private static boolean isTickTuple(Tuple tuple) {
			return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
		}

		public void tryWait(long INTERVAL){
			long start = System.nanoTime();
			long end=0;
			do{
				end = System.nanoTime();
			}while(start + INTERVAL >= end);
		}


		private void emit() {
			for (Map.Entry<String, Integer> entry : counts.entrySet()) {
				String str = entry.getKey();
				Integer count = entry.getValue();
				collector.emit(new Values(str, count));
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("key", "count"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			Map<String, Object> conf = new HashMap<String, Object>();
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, DEFAULT_TICK_FREQUENCY_SECONDS);
			return conf;
		}

		@Override
		public void cleanup() {
		}


		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
			this.collector = collector;
		}
    }

    public static class AggregatorBolt implements IRichBolt {
        private Map<String, Integer> counts = new HashMap<String, Integer>();
        private static final int DEFAULT_TICK_FREQUENCY_SECONDS = 1;
        private BigInteger tupleCount = BigInteger.valueOf(0);
        private int clockCount = 0;
        OutputCollector collector;
        Connection conn = null;
        
        @Override
        public void execute(Tuple tuple) {   	
            collector.ack(tuple);
        }

	      @Override
   	      public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	        declarer.declare(new Fields("word", "count"));
     	   }

		@Override
		public void cleanup() {
		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
            return null;
		}
    }


    public static void main(String[] args) throws Exception {
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("start", new WordGenerator(), 1);
        builder.setBolt("split", new SplitterBolt(), 32).shuffleGrouping("start").allGrouping("synopsis");     
	    builder.setBolt("coin", new CoinBolt(), 32).shuffleGrouping("split", "coin");
        builder.setBolt("synopsis", new PredictorBolt(),1).shuffleGrouping("coin");
        builder.setBolt("counter", new CounterBolt(), 32).fieldsGrouping("split", "nothot", new Fields("word")).shuffleGrouping("split", "hot");
        builder.setBolt("aggregator", new AggregatorBolt(), 1).fieldsGrouping("counter", new Fields("key"));

        Config conf = new Config();


        conf.setDebug(false);
        conf.setMaxSpoutPending(5000);   // maximum number tuples on the spout
		conf.setNumWorkers(8);
		StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
