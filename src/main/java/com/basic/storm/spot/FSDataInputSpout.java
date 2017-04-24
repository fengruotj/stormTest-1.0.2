package com.basic.storm.spot;

import com.basic.util.HdfsOperationUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * locate com.basic.storm.spot
 * Created by 79875 on 2017/4/20.
 */
public class FSDataInputSpout extends BaseRichSpout {

    private static Logger logger= LoggerFactory.getLogger(FSDataInputSpout.class);

    private String inputFile;

    private FSDataInputStream fsDataInputStream;

    private Fields outputFields;

    private static byte[] recordDelimiterBytes;//记录分割符
    private SplitLineReader splitLineReader;

    private SpoutOutputCollector outputCollector;
    public FSDataInputSpout(String inputFile) {
        this.inputFile = inputFile;
    }

    public FSDataInputSpout withOutputFields(String... fields) {
        this.outputFields = new Fields(fields);
        return this;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
        try {
            fsDataInputStream= HdfsOperationUtil.getFs().open(new Path(inputFile));
            splitLineReader=new SplitLineReader(fsDataInputStream,HdfsOperationUtil.getConf(),recordDelimiterBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        Text text = new Text();
        try {
            while (splitLineReader.readLine(text)!=0) {
                outputCollector.emit(new Values(text.toString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(this.outputFields);
    }
}
