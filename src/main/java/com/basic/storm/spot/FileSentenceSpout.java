package com.basic.storm.spot;

import com.basic.storm.model.SpoutResult;
import com.basic.util.PropertiesUtil;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by 79875 on 2017/2/23.
 */
public class FileSentenceSpout extends BaseRichSpout {
    private static Logger logger= LoggerFactory.getLogger(SentenceSpout.class);
    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";

    private static Timer timer;
    private static boolean m_bool=true;
    private long startTimemills;//开始时间 ms
    private long endTimemills;//结束时间 ms
    private static long spoutcount=0;
    private static Queue<SpoutResult> resultQueue=new ArrayDeque<SpoutResult>();

    private int index=0;
    private SpoutOutputCollector outputCollector;

    private ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    private BufferedReader in=null;

    static {
        timer=new Timer();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(!m_bool) {
                    resultQueue.add(new SpoutResult(System.currentTimeMillis(),spoutcount));
                    //executor.execute(new SpoutTupleCountTask(new Timestamp(System.currentTimeMillis()),spoutcount));
                    spoutcount = 0;
                }
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    //初始化操作
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        startTimemills=System.currentTimeMillis();
        logger.info("------------open------------");
        this.outputCollector=spoutOutputCollector;
        pending=new ConcurrentHashMap<UUID, Values>();

        try {
            in= new BufferedReader(new InputStreamReader(new FileInputStream(PropertiesUtil.getProperties("spoutSourceFilePath"))));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        m_bool=false;//让时间机器运行
    }

    //向下游输出
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word","timeinfo"));
        outputFieldsDeclarer.declareStream(TUPLECOUNT_STREAM_ID,new Fields("tuplecount","timeinfo"));
    }

    //核心逻辑
    public void nextTuple() {
//        Storm 的消息ack机制
//        Values value = new Values(sentences[index]);
//        UUID uuid=UUID.randomUUID();
//        pending.put(uuid,value);
//        this.outputCollector.emit(value,uuid);
//        //this.outputCollector.emit(value);
//        index++;
//        if(index>=sentences.length) index=0;
        String str="";
        try {
                str = in.readLine();
        } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
        }
        System.out.println(str);
        long currentTimemills=System.currentTimeMillis();//为了统计处理延迟
        outputCollector.emit(WORDCOUNT_STREAM_ID,new Values(str,currentTimemills));
        spoutcount++;

        //将输出结果发送给下游SpoutReport输出
        if(!resultQueue.isEmpty()){
            SpoutResult poll = resultQueue.poll();
            if(poll!=null)
                outputCollector.emit(TUPLECOUNT_STREAM_ID,new Values(poll.getSpoutcount(),poll.getSystemMills()));
        }
    }

    //Storm 的消息ack机制
//    @Override
//    public void ack(Object msgId) {
//        pending.remove(msgId);
//    }
//
//    @Override
//    public void fail(Object msgId) {
//        this.outputCollector.emit(pending.get(msgId),msgId);
//    }

    @Override
    public void close() {
          m_bool=true;
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        endTimemills=System.currentTimeMillis();
//        String txt1="spout传输的总数据量："+spoutcount;
//        String txt2="开始时间："+new Date(startTimemills)+" 结束时间："+new Date(endTimemills)+"经过时间(s)："+(endTimemills-startTimemills)/1000+"\r\n";
//        try {
//            FileUtil.writeAppendTxtFile(new File(PropertiesUtil.getProperties("spoutcountPath")),txt1);
//            FileUtil.writeAppendTxtFile(new File(PropertiesUtil.getProperties("spoutcountPath")),txt2);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}

