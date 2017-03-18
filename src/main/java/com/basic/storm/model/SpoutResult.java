package com.basic.storm.model;

/**
 * Created by 79875 on 2017/3/7.
 */
public class SpoutResult {
    private Long systemMills;
    private Long spoutcount;

    public SpoutResult() {
    }

    public SpoutResult(Long systemMills, Long spoutcount) {
        this.systemMills = systemMills;
        this.spoutcount = spoutcount;
    }

    public Long getSystemMills() {
        return systemMills;
    }

    public void setSystemMills(Long systemMills) {
        this.systemMills = systemMills;
    }

    public Long getSpoutcount() {
        return spoutcount;
    }

    public void setSpoutcount(Long spoutcount) {
        this.spoutcount = spoutcount;
    }
}
