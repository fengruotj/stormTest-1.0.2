package com.basic.storm.topology;

import com.basic.storm.bolt.HdfsWordCountBolt;
import com.basic.storm.bolt.report.ReportBolt;
import com.basic.storm.bolt.report.WordCountReportBolt;
import com.basic.storm.spot.HDFSBufferSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.IOException;

/**
 * locate com.basic.storm.topology
 * Created by 79875 on 2017/4/17.
 * 提交stormtopology任务 storm jar stormTest-1.0.2-SNAPSHOT.jar com.basic.storm.topology.HDFSBufferWordCountTopology hdfswordcount 8 16 /user/root/flinkwordcount/input/resultTweets.txt
 */
public class HDFSBufferWordCountTopology {
    public static final String HDFS_SPOUT_ID ="hdfs-buffer-spout";
    public static final String COUNT_BOLT_ID = "count-bolt";
    public static final String REPORT_BOLT_IT= "report-bolt";
    public static final String WORDCOUNT_REPORT_BOLT_ID= "wordcount-report-bolt";
    public static final String TOPOLOGY_NAME= "hdfs-wordcount-topology";
    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";

    public static void main(String[] args) throws IOException, InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        HDFSBufferSpout hdfsBufferSpout= new HDFSBufferSpout(10,args[3]).withOutputFields("word");

        HdfsWordCountBolt wordCountBolt=new HdfsWordCountBolt();
        ReportBolt reportBolt=new ReportBolt();
        WordCountReportBolt wordCountReportBolt=new WordCountReportBolt();

        TopologyBuilder builder=new TopologyBuilder();
        Integer numworkers=Integer.valueOf(args[1]);
        Integer wordcountboltparallelism=Integer.valueOf(args[2]);


        builder.setSpout(HDFS_SPOUT_ID,hdfsBufferSpout).setMemoryLoad(2048);
//        builder.setBolt(SPLIT_BOLT_ID,splitSentencesBolt,20)//splitSentencesBolt并发数
//                //.setNumTasks(4)//每个Executor中执行任务Task的数量,Task就是spout和bolt的实列
//                .shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID,wordCountBolt,wordcountboltparallelism)//1
                .fieldsGrouping(HDFS_SPOUT_ID,new Fields("word"));
                //.shuffleGrouping(HDFS_SPOUT_ID);
        builder.setBolt(REPORT_BOLT_IT,reportBolt)
                .shuffleGrouping(COUNT_BOLT_ID,WORDCOUNT_STREAM_ID);
        builder.setBolt(WORDCOUNT_REPORT_BOLT_ID,wordCountReportBolt)
                .allGrouping(COUNT_BOLT_ID,TUPLECOUNT_STREAM_ID);

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程 10
        config.setNumAckers(0);//每个Work进程会运行一个Acker任务，这里将Ack任务设置为0 禁止Ack任务
        config.setTopologyWorkerMaxHeapSize(2048);
        if(args[0].equals("local")){
            LocalCluster localCluster=new LocalCluster();

            localCluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
            Utils.sleep(50*1000);//50s
            localCluster.killTopology(TOPOLOGY_NAME);
            localCluster.shutdown();
        }else {
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }
    }
}
