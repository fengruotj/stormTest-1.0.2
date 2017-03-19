package com.basic.storm.model;

/**
 * Created by 79875 on 2017/3/19.
 */
public class HdfsResult {
    private Long systemMills;
    private Long bytecount;

    public HdfsResult() {
    }

    public HdfsResult(Long systemMills, Long bytecount) {
        this.systemMills = systemMills;
        this.bytecount = bytecount;
    }

    public Long getSystemMills() {
        return systemMills;
    }

    public void setSystemMills(Long systemMills) {
        this.systemMills = systemMills;
    }

    public Long getbytecount() {
        return bytecount;
    }

    public void setbytecount(Long bytecount) {
        this.bytecount = bytecount;
    }
}
