package com.basic.storm.model;

/**
 * Created by 79875 on 2017/3/8.
 */
public class BenchResult {
    private Long systemMills;
    private Long tuplecount;

    public BenchResult() {
    }

    public BenchResult(Long systemMills, Long tuplecount) {
        this.systemMills = systemMills;
        this.tuplecount = tuplecount;
    }

    public Long getSystemMills() {
        return systemMills;
    }

    public void setSystemMills(Long systemMills) {
        this.systemMills = systemMills;
    }

    public Long getTuplecount() {
        return tuplecount;
    }

    public void setTuplecount(Long tuplecount) {
        this.tuplecount = tuplecount;
    }
}
