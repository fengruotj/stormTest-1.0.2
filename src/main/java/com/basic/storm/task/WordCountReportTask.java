package com.basic.storm.task;

import com.basic.util.DataBaseUtil;

import java.sql.Timestamp;

/**
 * Created by 79875 on 2017/3/6.
 */
public class WordCountReportTask implements Runnable {

    private String word;

    private Long count;

    private Timestamp timestamp;

    public WordCountReportTask(Timestamp timestamp,String word,Long count){
        this.word=word;
        this.count=count;
        this.timestamp=timestamp;
    }

    public void run() {
        DataBaseUtil.insertWordCount(timestamp,word,count);
    }
}
