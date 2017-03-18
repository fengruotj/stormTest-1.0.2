package com.basic.storm.spot;

import com.basic.storm.model.SpoutResult;
import com.basic.storm.util.PropertiesUtil;
import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.*;

/**
 * Created by 79875 on 2017/3/11.
 */
public class SocketSentenceSpout  extends BaseRichSpout {
    private static Logger logger= LoggerFactory.getLogger(SocketSentenceSpout.class);

    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";

    private long startTimemills;//开始时间 ms
    private long endTimemills;//结束时间 ms
    private static long spoutcount=0;
    private static Timer timer;
    private static boolean m_bool=true;
    private static Queue<SpoutResult> resultQueue=new ArrayDeque<SpoutResult>();

    private ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    private SpoutOutputCollector outputCollector;

    private Socket sock=null;
    private BufferedReader in=null;

    static {
        timer=new Timer();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(!m_bool) {
                    //executor.execute(new SpoutTupleCountTask(new Timestamp(System.currentTimeMillis()),spoutcount));
                    resultQueue.add(new SpoutResult(System.currentTimeMillis(),spoutcount));
                    spoutcount = 0;
                }
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        if(sock==null){
            try {
                sock=new Socket(PropertiesUtil.getProperties("socketIPAddress"),Integer.valueOf(PropertiesUtil.getProperties("socketPort")));
                in= new BufferedReader(new InputStreamReader(sock.getInputStream()));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        startTimemills=System.currentTimeMillis();
        logger.info("------------open------------");
        this.outputCollector=spoutOutputCollector;
        pending=new ConcurrentHashMap<UUID, Values>();
        m_bool=false;//让时间机器运行
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word","timeinfo"));
        outputFieldsDeclarer.declareStream(TUPLECOUNT_STREAM_ID,new Fields("tuplecount","timeinfo"));
    }

    public void nextTuple() {

        String word = null;
        try {
            if(in!=null)
                word = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //如果word不等于空并且word不是""就执行发送逻辑
        if(word!=null && !word.equals("")){
            long currentTimemills=System.currentTimeMillis();//为了统计处理延迟
            outputCollector.emit(WORDCOUNT_STREAM_ID,new Values(word,currentTimemills));
            spoutcount++;
        }

        //将输出结果发送给下游SpoutReport输出
        if(!resultQueue.isEmpty()){
            SpoutResult poll = resultQueue.poll();
            if(poll!=null)
                outputCollector.emit(TUPLECOUNT_STREAM_ID,new Values(poll.getSpoutcount(),poll.getSystemMills()));
        }
    }
}
