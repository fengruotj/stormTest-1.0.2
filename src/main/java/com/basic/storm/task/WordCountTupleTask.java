package com.basic.storm.task;

import com.basic.util.DataBaseUtil;

import java.sql.Timestamp;

/**
 * Created by 79875 on 2017/3/6.
 */
public class WordCountTupleTask implements Runnable {

    private Long tupplecount;

    private Timestamp timestamp;

    public WordCountTupleTask(Timestamp timestamp,Long tupplecount){
        this.timestamp=timestamp;
        this.tupplecount=tupplecount;
    }

    public void run() {
        DataBaseUtil.insertTupleCount(timestamp,tupplecount);
    }
}
