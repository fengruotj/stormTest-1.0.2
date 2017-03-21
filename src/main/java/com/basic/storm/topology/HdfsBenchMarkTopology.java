package com.basic.storm.topology;

import com.basic.storm.hdfsbenchmark.HdfsComputeBolt;
import com.basic.storm.hdfsbenchmark.HdfsReportBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.spout.Configs;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by 79875 on 2017/3/19.
 * 提交stormtopology任务 storm jar stormTest-1.0.2-SNAPSHOT.jar com.basic.storm.topology.HdfsBenchMarkTopology hdfsbenchmark 1 1 1 /benchmarks/TestDFSIO/io_data /user/root/output /user/root/baddir
 */
public class HdfsBenchMarkTopology {

    public static final String HDFS_SPOUT_ID ="hdfs-spout";
    public static final String HDFS_BOLT_ID ="hdfs-bolt";
    public static final String COMPUTE_BOLT_ID = "compute-bolt";
    public static final String REPORT_BOLT_IT= "report-bolt";
    public static final String TOPOLOGY_NAME= "hdfs-benchMark-topology";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        String hdfsUri = "hdfs://root2:9000";
        HdfsSpout hdfsSpout=new HdfsSpout().withOutputFields("word");

        HdfsComputeBolt computeBolt=new HdfsComputeBolt();
        HdfsReportBolt reportBolt=new HdfsReportBolt();

        /*
        // Configure HDFS bolt
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("\t"); // use "\t" instead of "," for field delimiter
        SyncPolicy syncPolicy = new CountSyncPolicy(1000); // sync the filesystem after every 1k tuples
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES); // rotate files
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/storm/").withPrefix("app_").withExtension(".log"); // set file name format
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl(hdfsUri)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
*/
        TopologyBuilder builder=new TopologyBuilder();

        Integer numworkers=Integer.valueOf(args[1]);
        Integer spoutparallelism=Integer.valueOf(args[2]);
        Integer computeboltparallelism=Integer.valueOf(args[3]);

        // 1 - parse cmd line args
        String fileFormat = "TEXT";
        String sourceDir = args[4];
        String sourceArchiveDir = args[5];
        String badDir = args[6];


        builder.setSpout(HDFS_SPOUT_ID,hdfsSpout,spoutparallelism);//10 spout并发数
        builder.setBolt(COMPUTE_BOLT_ID,computeBolt,computeboltparallelism)
                //.setNumTasks(4)//每个Executor中执行任务Task的数量,Task就是spout和bolt的实列
                .shuffleGrouping(HDFS_SPOUT_ID);

        builder.setBolt(REPORT_BOLT_IT,reportBolt)
                .globalGrouping(COMPUTE_BOLT_ID);
/*
        builder.setBolt(HDFS_BOLT_ID,hdfsBolt,2)
                .shuffleGrouping(COMPUTE_BOLT_ID);
*/

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程
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
            Utils.sleep(5*60*1000);//50s
            localCluster.killTopology(TOPOLOGY_NAME);
            localCluster.shutdown();
        }else {
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }

    }
}
