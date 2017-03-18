package com.basic.storm.topology;

import com.basic.storm.benchmarkbolt.ComputeBolt;
import com.basic.storm.benchmarkbolt.ParserBolt;
import com.basic.storm.bolt.report.SpoutReportBolt;
import com.basic.storm.bolt.report.WordCountReportBolt;
import com.basic.storm.spot.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by dello on 2016/10/15.
 * Storm处理层什么逻辑都不做的BenchMark集群测试
 * 提交stormtopology任务 storm jar stormTest-1.0.2-SNAPSHOT-jar-with-dependencies.jar com.basic.storm.topology.BenchMarkTopology stormbenchmark 10 10 10
 */
public class BenchMarkTopology {
    public static final String SENTENCE_SPOUT_ID ="sentence-spout";
    public static final String PARSER_BOLT_ID = "parser-bolt";
    public static final String COMPUTE_BOLT_ID= "compute-bolt";
    public static final String WORDCOUNT_REPORT_BOLT_ID= "wordcount-report-bolt";
    public static final String SPOUT_REPORT_BOLT_ID= "spout-report-bolt";
    public static final String TOPOLOGY_NAME= "word-benchmark-topology";
    public static final String COMPUTE_STREAM_ID="computestream";
    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        SentenceSpout spout=new SentenceSpout();
        ComputeBolt computeBolt=new ComputeBolt();
        ParserBolt parserBolt=new ParserBolt();
        WordCountReportBolt wordCountReportBolt=new WordCountReportBolt();
        SpoutReportBolt spoutReportBolt=new SpoutReportBolt();

        TopologyBuilder builder=new TopologyBuilder();

        Integer numworkers=Integer.valueOf(args[1]);
        Integer spoutparallelism=Integer.valueOf(args[2]);
        Integer parserboltparallelism=Integer.valueOf(args[3]);

        builder.setSpout(SENTENCE_SPOUT_ID,spout,spoutparallelism);//10 spout并发数
        builder.setBolt(PARSER_BOLT_ID,parserBolt,parserboltparallelism)//splitSentencesBolt并发数
                //.setNumTasks(4)//每个Executor中执行任务Task的数量,Task就是spout和bolt的实列
                .shuffleGrouping(SENTENCE_SPOUT_ID,WORDCOUNT_STREAM_ID);

        builder.setBolt(COMPUTE_BOLT_ID,computeBolt)
                .globalGrouping(PARSER_BOLT_ID,COMPUTE_STREAM_ID);

        builder.setBolt(SPOUT_REPORT_BOLT_ID,spoutReportBolt)
                .allGrouping(SENTENCE_SPOUT_ID,TUPLECOUNT_STREAM_ID);
        builder.setBolt(WORDCOUNT_REPORT_BOLT_ID,wordCountReportBolt)
                .allGrouping(PARSER_BOLT_ID,TUPLECOUNT_STREAM_ID);

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程

        if(args[0].equals("local")){
            LocalCluster localCluster=new LocalCluster();

            localCluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
            Utils.sleep(5*60*1000);//50s
            localCluster.killTopology(TOPOLOGY_NAME);
            localCluster.shutdown();
        }else {
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }

    }
}
