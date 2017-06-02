package com.basic.storm.hdfsbenchmark;

import com.basic.storm.model.HdfsResult;
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

import java.sql.SQLException;
import java.util.*;

/**
 * Created by 79875 on 2017/3/19.
 */
public class HdfsComputeBolt extends BaseRichBolt {

    private static Logger logger= LoggerFactory.getLogger(HdfsComputeBolt.class);

    private static Timer timer;
    private static long bytecount=0; //记录单位时间通过的元组数量
    private static long allbytecount=0;//记录所有通过本Test的元组数量
    private static Queue<HdfsResult> resultQueue=new ArrayDeque<HdfsResult>();
    private static boolean m_bool=true;//判断使计时器合理运行

    private long startTimemills;//开始时间 ms
    private OutputCollector outputCollector;
    private long delaystartTimemills;//处理逻辑起始时间 用来统计延迟
    private long delayendTimemills;//处理逻辑结束时间  用来统计延迟
    static {
        timer=new Timer();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(!m_bool) {
                    //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),bytecount));
                    resultQueue.add(new HdfsResult(System.currentTimeMillis(),bytecount));
                    bytecount = 0;
                }
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        logger.info("------------ HdfsComputeBolt prepare------------");
        this.outputCollector=outputCollector;
        startTimemills=System.currentTimeMillis();
        m_bool=false;//让计时器运行
    }

    @Override
    public void execute(Tuple tuple) {
        //将数据写入文件
        String word = tuple.getStringByField("word");
        //int length = word.getBytes().length;
        int length = 1;
        bytecount=bytecount+length;
        allbytecount=allbytecount+length;

        //将输出结果发送给下游SpoutReport输出
        if(!resultQueue.isEmpty()){
            HdfsResult poll = resultQueue.poll();
            if(poll!=null){
                outputCollector.emit(tuple,new Values(poll.getbytecount(),poll.getSystemMills()));
                logger.debug("------------ HdfsComputeBolt emit bytecountstream------------"+" time:"+poll.getSystemMills()+" bytecount:"+poll.getbytecount());
            }
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("bytecount","timeinfo"));
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
