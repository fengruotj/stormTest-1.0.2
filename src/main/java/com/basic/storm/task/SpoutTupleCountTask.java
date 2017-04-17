package com.basic.storm.task;

import com.basic.util.DataBaseUtil;

import java.sql.Timestamp;

/**
 * Created by 79875 on 2017/3/6.
 */
public class SpoutTupleCountTask implements Runnable {

    private Long spoutcount;

    private Timestamp timestamp;

    public SpoutTupleCountTask(Timestamp timestamp,Long spoutcount){
        this.timestamp=timestamp;
        this.spoutcount=spoutcount;
    }

    public void run() {
        DataBaseUtil.insertSpoutTupleCount(timestamp,spoutcount);
    }
}
