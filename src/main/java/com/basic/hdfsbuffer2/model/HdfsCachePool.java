package com.basic.hdfsbuffer2.model;

import com.basic.hdfsbuffer2.task.DataInputTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 79875 on 2017/4/1.'
 */
public class  HdfsCachePool {
    private static final Log LOG = LogFactory.getLog(HdfsCachePool.class);
    private static HdfsCachePool instance;//缓存池唯一实例

    private HDFSBuffer[] bufferArray;

    private int bufferNum = 10;

    private int positionBlock = 0;    //当前读取HDFS文件Block下标

    private int loopNum=0;  //缓冲轮询次数

    private int loopTmp=0; //维护loopNum 的辅助变量

    private List<InputSplit> inputSplitList = new ArrayList<>();

    private int activeBufferNum=0;

    public int getActiveBufferNum() {
        return activeBufferNum;
    }

    //    private boolean isbufferfinish=false;   //是否缓存数据完毕
    /**
     * 线程池
     */
    ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 20, 200, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    public HdfsCachePool(int bufferNum, List<InputSplit> inputSplitList) throws IOException, InterruptedException {
        this.bufferNum = bufferNum;
        this.inputSplitList = inputSplitList;
        bufferArray = new HDFSBuffer[bufferNum];
        for(int i=0;i<bufferNum;i++){
            bufferArray[i]=new HDFSBuffer();
        }
    }

    /**
     * 初始化HDFSCachePool 缓冲池
     *
     * @param inputSplitList
     * @throws IOException
     * @throws InterruptedException
     */
    public void init(List<InputSplit> inputSplitList) throws IOException, InterruptedException {
//        if(bufferArray!=null)//如果缓冲数组不为空则首先清除缓冲区
//            clearBufferArray();
        for(int i=0;i<bufferNum;i++){
            bufferArray[i]=new HDFSBuffer();
        }
        for (int i = 0; i < bufferNum; i++) {
            bufferArray[i].byteBuffer = ByteBuffer.allocate((int) inputSplitList.get(i).getLength());//创建一个128M大小的字节缓存区
        }
    }

    public void setInstance(int bufferindex, InputSplit inputSplit) throws IOException, InterruptedException {
        bufferArray[bufferindex].setBufferFinished(false);
        bufferArray[bufferindex].byteBuffer = ByteBuffer.allocate((int) inputSplit.getLength());
    }


    /**
     * 得到唯一实例
     *
     * @return
     */
    public synchronized static HdfsCachePool getInstance() {
//        if(instance == null){
//            instance = new HdfsCachePool();
//        }
        return instance;
    }

    public synchronized static HdfsCachePool getInstance(int bufferNum, List<InputSplit> inputSplitList) throws IOException, InterruptedException {
        instance = new HdfsCachePool(bufferNum, inputSplitList);
        return instance;
    }

    /**
     * 缓存bufferBlock缓冲池数据块
     *
     * @param bufferBlock bufferBlock块编号
     * @param inputSplit  hdfs文件分片
     * @throws IOException
     */
    public void datainputBuffer(int bufferBlock, InputSplit inputSplit, int blockNum) throws IOException, InterruptedException {
        setInstance(bufferBlock,inputSplit);
        DataInputTask dataInputTask = new DataInputTask(this.bufferArray[bufferBlock], inputSplit,blockNum);
        executor.execute(dataInputTask);
    }

    public HDFSBuffer[] getBufferArray() {
        return bufferArray;
    }


    /**
     * clear缓冲区数组
     * position = 0;
     * limit = capacity;
     * mark = -1;
     */
    public void clearBufferArray() {
        for (int i = 0; i < bufferArray.length; i++) {
            bufferArray[i].byteBuffer.clear();
        }
    }

    public boolean isIsbufferfinish() {
        if(loopNum==0 || executor.getActiveCount()>0)
            return false;
        else
            return true;
    }


    /**
     * 缓冲区输出完毕继续缓存下一块Block
     * @param Num 当前输出缓冲块的下标
     */
    public void bufferNextBlock(int Num) throws IOException, InterruptedException {
        LOG.debug("bufferNextBlock------------ NUM: "+Num+" inputSplitNum:"+(Num+bufferNum*loopNum)+" loopNum: "+loopNum);
        datainputBuffer(Num,inputSplitList.get(Num+bufferNum*loopNum),Num+bufferNum*loopNum);
        loopTmp++;
        positionBlock++;
        if(loopTmp % bufferNum ==0){
            //已经buffer缓存轮询一次 loopNum++
            loopNum++;
        }
    }

    /**
     * 是否buferBlock缓存输出完毕
     * @param blocknum blocknum下标
     * @return
     */
    public boolean isBufferBlockoutFinished(int blocknum){
        ByteBuffer byteBuffer = bufferArray[blocknum].byteBuffer;
        if(byteBuffer.hasRemaining())
            return false;
        else return true;
    }

    /**
     *  是否buferBlock缓存完毕
     * @param blocknum blocknum下标
     * @return
     */
    public boolean isBufferBlockFinished(int blocknum){
        return bufferArray[blocknum].isBufferFinished();
    }

    public List<InputSplit> getInputSplitList() {
        return inputSplitList;
    }

    public void setInputSplitList(List<InputSplit> inputSplitList) {
        this.inputSplitList = inputSplitList;
    }

    public class HdfsCachePoolControlTask implements Runnable{

        @Override
        public void run() {
                while (true){
                    try {
                        int length=(inputSplitList.size()/bufferNum == loopNum)? inputSplitList.size()%bufferNum : bufferNum;
                        for(int i=0;i<bufferNum;i++){
                            if(loopNum==0 || isBufferBlockoutFinished(i)){
                                if(i<length){
                                    //如果i小于当前剩余的split数量。将缓存当前split
                                    activeBufferNum=length;
                                    bufferNextBlock(i);
                                }
                            }
                        }
                        if(positionBlock>=inputSplitList.size()){
                            //bufferoutputOver=true;
                            LOG.info("----------------dataInput over--------------"+" activeBufferNum:"+activeBufferNum);
                            break;
                        }
                        Thread.sleep(100);
                } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        }
    }

//    public class HdfsCachePoolExecutorTask implements Runnable {
//
//        @Override
//        public void run() {
//            try {
//                for (int i = 0; i < bufferNum; i++) {
//                    setInstance(i,inputSplitList.get(i));
//                    DataInputTask dataInputTask = new DataInputTask(bufferArray[i], inputSplitList.get(i));
//                    executor.execute(dataInputTask);
//                    positionBlock++;
//                }
//                loopNum++;
//                isbufferfinish=true;
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }

    /**
     * 启动HdfsCachePool 缓充池
     */
    public void runHDFSCachePool(){
        HdfsCachePoolControlTask hdfsCachePoolControlTask=new HdfsCachePoolControlTask();
        Thread thread2 = new Thread(hdfsCachePoolControlTask);
        thread2.start();
    }
}
