package com.basic.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by 79875 on 2017/3/15.
 */
public class NumPartitioner implements Partitioner {

    public NumPartitioner(VerifiableProperties props) {
    }

    public int partition(Object key, int numPartitions) {
        Integer partition= (Integer) key;
        return partition;
    }
}
