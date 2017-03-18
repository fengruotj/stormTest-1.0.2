package com.basic.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by 79875 on 2017/3/3.
 * KafkaBolt 用来接收从Kafka传来的数据
 */
public class KaffkaBolt extends BaseRichBolt {

    private Logger logger= LoggerFactory.getLogger(KaffkaBolt.class);
    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    public void execute(Tuple tuple) {
        long startTimemills= System.currentTimeMillis();
        String words=tuple.getString(0);
        outputCollector.emit(new Values(words,startTimemills));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","timeinfo"));
    }
}
