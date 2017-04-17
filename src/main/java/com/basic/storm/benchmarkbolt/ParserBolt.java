package com.basic.storm.benchmarkbolt;

import com.basic.storm.model.BenchResult;
import com.basic.util.DataBaseUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by 79875 on 2017/2/18.
 */
public class ParserBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(ParserBolt.class);

    public static final String COMPUTE_STREAM_ID="computestream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";

    private static Timer timer;
    private static boolean m_bool=true;//判断使计时器合理运行
    private static long tupplecount=0; //记录单位时间通过的元组数量
    private static long alltupplecount=0;//记录所有通过本Test的元组数量
    private static Queue<BenchResult> resultQueue=new ArrayDeque<BenchResult>();

    static {
        timer=new Timer();
        DataBaseUtil.getConnection();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(!m_bool) {
                    resultQueue.add(new BenchResult(System.currentTimeMillis(),tupplecount));
                    tupplecount = 0;
                }
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        m_bool=false;//让计时器运行
    }

    public void execute(Tuple tuple) {
        tupplecount++;
        alltupplecount++;
        String sentences=tuple.getStringByField("word");
        outputCollector.emit(COMPUTE_STREAM_ID,new Values(sentences,System.currentTimeMillis()));

        //将输出结果发送给下游Report输出
        if(resultQueue.size()>10){
            while (!resultQueue.isEmpty()){
                BenchResult poll = resultQueue.poll();
                outputCollector.emit(TUPLECOUNT_STREAM_ID,new Values(poll.getTuplecount() ,poll.getSystemMills()));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(COMPUTE_STREAM_ID,new Fields("word","timeinfo"));
        outputFieldsDeclarer.declareStream(TUPLECOUNT_STREAM_ID,new Fields("tuplecount","timeinfo"));
    }

    @Override
    public void cleanup() {
        timer.cancel();
    }

}
