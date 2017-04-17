package com.basic.hdfsbuffer2;

import com.basic.hdfsbuffer2.model.HdfsCachePool;
import com.basic.util.KafkaUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * locate com.basic.hdfsbuffer2
 * Created by 79875 on 2017/4/11.
 * HDFSCachePoll 的数据输出类 数据输出到Kafka中
 */
public class DataOutputKafka {
    private static int threadNum=1;
    private static KafkaUtil kafkaUtil=new KafkaUtil(threadNum);

    private static final Log LOG = LogFactory.getLog(DataOutputKafka.class);
    private long Totalrows=0L;
    private long blockrows=0L;
    private HdfsCachePool hdfsCachePool;

    private int blockPosition=0;

    private  int kafkaParitionsNum;

    public DataOutputKafka(HdfsCachePool hdfsCachePool,int kafkaParitionsNum) {
        this.hdfsCachePool = hdfsCachePool;
        this.kafkaParitionsNum=kafkaParitionsNum;
    }

    public void datoutputKafka(String kafkatopic) throws IOException, InterruptedException {
        while (true){
            if(hdfsCachePool.isIsbufferfinish()){
                //可以开始读取HdfsCachePool
                int activeBufferNum = hdfsCachePool.getActiveBufferNum();
                for(int i=0;i<activeBufferNum;i++){
                    BufferLineReader bufferLineReader=new BufferLineReader(hdfsCachePool.getBufferArray()[i]);
                    Text text=new Text();
                    System.out.println("-----------------"+ hdfsCachePool.getBufferArray()[i] +" num:"+i+" blockPosition: "+blockPosition);
                    long startTimeSystemTime= System.currentTimeMillis();
                    while (bufferLineReader.readLine(text)!=0){
                        Totalrows++;
                        blockrows++;
                        //kafkaUtil.publishMessage(kafkatopic, String.valueOf(Totalrows),text.toString());
                        kafkaUtil.publishOrderMessage(kafkatopic,kafkaParitionsNum,(int)Totalrows,text.toString());
                        //System.out.println(text.toString());
                        //System.out.println(byteBuffer);
                    }
                    System.out.println("BlockRows : "+blockrows);
                    blockPosition++;
                    blockrows=0;
                }
                if(blockPosition>=hdfsCachePool.getInputSplitList().size()){
                    System.out.println("Totalrows : "+Totalrows);
                    LOG.info("----------------dataOuput over--------------");
                    break;
                }
            }
            Thread.sleep(100);
        }
    }
}
