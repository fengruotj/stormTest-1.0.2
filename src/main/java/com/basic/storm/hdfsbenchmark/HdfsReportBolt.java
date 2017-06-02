package com.basic.storm.hdfsbenchmark;

import com.basic.storm.task.WordCountTupleTask;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 79875 on 2017/3/19.
 */
public class HdfsReportBolt extends BaseRichBolt {

    private Logger logger= LoggerFactory.getLogger(HdfsReportBolt.class);
    private OutputCollector outputCollector;
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 200, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        logger.info("------------ReportBolt prepare------------");
        //DataBaseUtil.getConnection();
    }

    public void execute(Tuple tuple) {
        Long currentTimeMills=tuple.getLongByField("timeinfo");
        Long bytecount=tuple.getLongByField("bytecount");

        //将最后结果插入到数据库中
        Timestamp timestamp=new Timestamp(currentTimeMills);
        WordCountTupleTask wordCountTupleTask=new WordCountTupleTask(timestamp,bytecount);
        executor.execute(wordCountTupleTask);
        logger.debug("timestamp:"+currentTimeMills+" bytecount:"+bytecount);
        outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {

    }
}

