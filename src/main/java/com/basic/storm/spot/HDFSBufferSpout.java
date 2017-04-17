package com.basic.storm.spot;

import com.basic.hdfsbuffer2.BufferLineReader;
import com.basic.hdfsbuffer2.model.HdfsCachePool;
import com.basic.hdfsbuffer2.task.DataInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * locate com.basic.storm.spot
 * Created by 79875 on 2017/4/17.
 */
public class HDFSBufferSpout extends BaseRichSpout {
    private static Logger logger= LoggerFactory.getLogger(HDFSBufferSpout.class);

    private Fields outputFields;

    private long Totalrows=0L;
    private long blockrows=0L;
    private HdfsCachePool hdfsCachePool;

    private int blockPosition=0;

    private SpoutOutputCollector outputCollector;

    private int bufferNum;
    private String inputFile;

    public HDFSBufferSpout() {

    }

    public HDFSBufferSpout(int bufferNum, String inputFile) throws IOException, InterruptedException {
        this.bufferNum=bufferNum;
        this.inputFile=inputFile;
    }

    public HDFSBufferSpout withOutputFields(String... fields) {
        this.outputFields = new Fields(fields);
        return this;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            DataInputFormat dataInputFormat=new DataInputFormat();
            List<InputSplit> splits = dataInputFormat.getSplits(inputFile);
            this.hdfsCachePool=HdfsCachePool.getInstance(10,splits);
        } catch (Exception e) {
            e.printStackTrace();
        }
        hdfsCachePool.runHDFSCachePool();
        this.outputCollector=spoutOutputCollector;
    }

    @Override
    public void nextTuple(){
        try {
            datoutputTuple();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void datoutputTuple() throws IOException, InterruptedException {
        while (true){
            if(hdfsCachePool.isIsbufferfinish()){
                //可以开始读取HdfsCachePool
                int activeBufferNum = hdfsCachePool.getActiveBufferNum();
                for(int i=0;i<activeBufferNum;i++){
                    BufferLineReader bufferLineReader=new BufferLineReader(hdfsCachePool.getBufferArray()[i]);
                    Text text=new Text();
                    logger.info("-----------------"+ hdfsCachePool.getBufferArray()[i] +" num:"+i+" blockPosition: "+blockPosition);
                    long startTimeSystemTime= System.currentTimeMillis();
                    while (bufferLineReader.readLine(text)!=0){
                        Totalrows++;
                        blockrows++;
                        //kafkaUtil.publishMessage(kafkatopic, String.valueOf(Totalrows),text.toString());
                        outputCollector.emit(new Values(text.toString()));
                        //System.out.println(text.toString());
                        //System.out.println(byteBuffer);
                    }
                    logger.info("BlockRows : "+blockrows);
                    blockPosition++;
                    blockrows=0;
                }
                if(blockPosition>=hdfsCachePool.getInputSplitList().size()){
                    logger.info("Totalrows : "+Totalrows);
                    logger.info("----------------dataOuput over--------------");
                    break;
                }
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(this.outputFields);
    }
}
