package com.basic.storm.bolt;

import com.basic.storm.model.WordCountResult;
import com.basic.storm.util.DataBaseUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by dello on 2016/10/15.
 */
public class KafkaWordCountBolt extends BaseRichBolt{

    private static Logger logger= LoggerFactory.getLogger(KafkaWordCountBolt.class);

    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";

    private static Timer timer;
    private static long tupplecount=0; //记录单位时间通过的元组数量
    private static long alltupplecount=0;//记录所有通过本Test的元组数量
    private static Queue<WordCountResult> resultQueue=new ArrayDeque<WordCountResult>();
    private static boolean m_bool=true;//判断使计时器合理运行

    private long startTimemills;//开始时间 ms
    private long endTimemills;//结束时间 ms
    private OutputCollector outputCollector;
    private long delaystartTimemills;//处理逻辑起始时间 用来统计延迟
    private long delayendTimemills;//处理逻辑结束时间  用来统计延迟
    private HashMap<String,Long> counts=null;

    static {
        timer=new Timer();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(!m_bool) {
                    //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),tupplecount));
                    resultQueue.add(new WordCountResult(System.currentTimeMillis(),tupplecount));
                    tupplecount = 0;
                }
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        logger.info("------------prepare------------");
        this.outputCollector=outputCollector;
        startTimemills=System.currentTimeMillis();
        m_bool=false;//让计时器运行
        this.counts=new HashMap<String, Long>();
    }

    //可靠的单词计数Topology
    public void execute(Tuple tuple) {

        //将数据写入文件
        tupplecount++;
        alltupplecount++;

        String word=tuple.getStringByField("str");
        Long counts=this.counts.get(word);
        if(counts==null){
            counts=0L;
        }
        counts++;
        this.counts.put(word,counts);
        outputCollector.emit(WORDCOUNT_STREAM_ID,new Values(word,counts));
        //logger.info("------------ KafkaWordCountBolt emit wordcounStream------------"+" word:"+word+" count:"+counts);
        //屏蔽代码
        delayendTimemills=System.currentTimeMillis();
        //将算出延迟写入文件
        Long delay=delayendTimemills-delaystartTimemills;

        //将输出结果发送给下游SpoutReport输出
        if(!resultQueue.isEmpty()){
            WordCountResult poll = resultQueue.poll();
            if(poll!=null){
                outputCollector.emit(TUPLECOUNT_STREAM_ID,new Values(poll.getTupplecount(),poll.getSystemMills()));
                logger.info("------------ KafkaWordCountBolt emit tupplecountstream------------"+" time:"+poll.getSystemMills()+" tupplecount:"+poll.getTupplecount());
            }

        }

        //Strom 消息ack机制
//        this.outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word","count"));
        outputFieldsDeclarer.declareStream(TUPLECOUNT_STREAM_ID,new Fields("tuplecount","timeinfo"));
    }

    @Override
    public void cleanup() {
        try {
            DataBaseUtil.closeconnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        timer.cancel();
    }
}
