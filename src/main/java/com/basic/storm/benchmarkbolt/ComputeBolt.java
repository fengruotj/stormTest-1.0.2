package com.basic.storm.benchmarkbolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by 79875 on 2017/2/18.
 */
public class ComputeBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(ComputeBolt.class);

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


}
