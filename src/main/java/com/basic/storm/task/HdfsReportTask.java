package com.basic.storm.task;

import com.basic.storm.util.DataBaseUtil;

import java.sql.Timestamp;

/**
 * Created by 79875 on 2017/3/19.
 */
public class HdfsReportTask implements Runnable {

    private Long bytecount;

    private Timestamp timestamp;

    public HdfsReportTask(Timestamp timestamp,Long bytecount){
        this.timestamp=timestamp;
        this.bytecount=bytecount;
    }

    public void run() {
        DataBaseUtil.insertTupleCount(timestamp,bytecount);
    }
}
