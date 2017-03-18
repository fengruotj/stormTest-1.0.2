package com.basic.storm.topology;

import com.basic.storm.bolt.WordCountBolt;
import com.basic.storm.bolt.report.ReportBolt;
import com.basic.storm.bolt.report.SpoutReportBolt;
import com.basic.storm.bolt.report.WordCountReportBolt;
import com.basic.storm.spot.FileSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by dello on 2016/10/15.
 * 提交stormtopology任务 storm jar stormTest-1.0.1-SNAPSHOT-jar-with-dependencies.jar com.basic.storm.topology.WordCountTopology stormwordcount 5 5 10 1
 */
public class WordCountTopology {
    public static final String SENTENCE_SPOUT_ID ="sentence-spout";
    public static final String  SPLIT_BOLT_ID ="split-bolt";
    public static final String COUNT_BOLT_ID = "count-bolt";
    public static final String REPORT_BOLT_ID= "report-bolt";
    public static final String SPOUT_REPORT_BOLT_ID= "spout-report-bolt";
    public static final String WORDCOUNT_REPORT_BOLT_ID= "wordcount-report-bolt";
    public static final String TOPOLOGY_NAME= "word-count-topology";
    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        FileSentenceSpout spout=new FileSentenceSpout();
        WordCountBolt wordCountBolt=new WordCountBolt();
        ReportBolt reportBolt=new ReportBolt();
        SpoutReportBolt spoutReportBolt=new SpoutReportBolt();
        WordCountReportBolt wordCountReportBolt=new WordCountReportBolt();

        TopologyBuilder builder=new TopologyBuilder();
        Integer numworkers=Integer.valueOf(args[1]);
        Integer spoutparallelism=Integer.valueOf(args[2]);
        Integer wordcountboltparallelism=Integer.valueOf(args[3]);
        Integer reportboltparallelism=Integer.valueOf(args[4]);

        builder.setSpout(SENTENCE_SPOUT_ID,spout,spoutparallelism);//10 spout并发数
//        builder.setBolt(SPLIT_BOLT_ID,splitSentencesBolt,20)//splitSentencesBolt并发数
//                //.setNumTasks(4)//每个Executor中执行任务Task的数量,Task就是spout和bolt的实列
//                .shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID,wordCountBolt,wordcountboltparallelism)//1
                .fieldsGrouping(SENTENCE_SPOUT_ID,WORDCOUNT_STREAM_ID,new Fields("word"));

        builder.setBolt(REPORT_BOLT_ID,reportBolt,reportboltparallelism)
                .allGrouping(COUNT_BOLT_ID,WORDCOUNT_STREAM_ID);
        builder.setBolt(SPOUT_REPORT_BOLT_ID,spoutReportBolt)
                .allGrouping(SENTENCE_SPOUT_ID,TUPLECOUNT_STREAM_ID);
        builder.setBolt(WORDCOUNT_REPORT_BOLT_ID,wordCountReportBolt)
                .allGrouping(COUNT_BOLT_ID,TUPLECOUNT_STREAM_ID);

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程 10
        config.setNumAckers(0);//每个Work进程会运行一个Acker任务，这里将Ack任务设置为0 禁止Ack任务
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
