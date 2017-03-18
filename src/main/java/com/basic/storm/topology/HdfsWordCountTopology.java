package com.basic.storm.topology;

import com.basic.storm.bolt.HdfsWordCount;
import com.basic.storm.bolt.report.ReportBolt;
import com.basic.storm.bolt.report.WordCountReportBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.spout.Configs;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by 79875 on 2017/3/18.
 * 提交stormtopology任务 storm jar stormTest-1.0.2-SNAPSHOT.jar com.basic.storm.topology.HdfsWordCountTopology hdfswordcount 8 8 16 1 /user/root/input /user/root/output /user/root/baddir

 */
public class HdfsWordCountTopology {
    public static final String HDFS_SPOUT_ID ="hdfs-spout";
    public static final String COUNT_BOLT_ID = "count-bolt";
    public static final String REPORT_BOLT_IT= "report-bolt";
    public static final String WORDCOUNT_REPORT_BOLT_ID= "wordcount-report-bolt";
    public static final String TOPOLOGY_NAME= "hdfs-wordcount-topology";
    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        HdfsSpout hdfsSpout=new HdfsSpout().withOutputFields("word");

        HdfsWordCount wordCountBolt=new HdfsWordCount();
        ReportBolt reportBolt=new ReportBolt();
        WordCountReportBolt wordCountReportBolt=new WordCountReportBolt();

        TopologyBuilder builder=new TopologyBuilder();
        Integer numworkers=Integer.valueOf(args[1]);
        Integer spoutparallelism=Integer.valueOf(args[2]);
        Integer wordcountboltparallelism=Integer.valueOf(args[3]);
        Integer reportboltparallelism=Integer.valueOf(args[4]);

        // 1 - parse cmd line args
        String hdfsUri = "hdfs://root2:9000";
        String fileFormat = "TEXT";
        String sourceDir = args[5];
        String sourceArchiveDir = args[6];
        String badDir = args[7];

        builder.setSpout(HDFS_SPOUT_ID,hdfsSpout,spoutparallelism);//10 spout并发数
//        builder.setBolt(SPLIT_BOLT_ID,splitSentencesBolt,20)//splitSentencesBolt并发数
//                //.setNumTasks(4)//每个Executor中执行任务Task的数量,Task就是spout和bolt的实列
//                .shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID,wordCountBolt,wordcountboltparallelism)//1
                .fieldsGrouping(HDFS_SPOUT_ID,new Fields("word"));

        builder.setBolt(REPORT_BOLT_IT,reportBolt,reportboltparallelism)
                .shuffleGrouping(COUNT_BOLT_ID,WORDCOUNT_STREAM_ID);
        builder.setBolt(WORDCOUNT_REPORT_BOLT_ID,wordCountReportBolt)
                .allGrouping(COUNT_BOLT_ID,TUPLECOUNT_STREAM_ID);

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程 10
        config.setNumAckers(0);//每个Work进程会运行一个Acker任务，这里将Ack任务设置为0 禁止Ack任务

        /**
         * 设置HDFS相关配置
         */
        config.put(Configs.SOURCE_DIR, sourceDir);
        config.put(Configs.ARCHIVE_DIR, sourceArchiveDir);
        config.put(Configs.BAD_DIR, badDir);
        config.put(Configs.READER_TYPE, fileFormat);
        config.put(Configs.HDFS_URI, hdfsUri);

        config.setDebug(true);

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
