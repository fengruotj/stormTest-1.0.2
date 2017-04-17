package com.basic.hdfsbuffer2;

import org.junit.Test;

/**
 * locate com.basic.hdfsbuffer2
 * Created by 79875 on 2017/4/17.
 */
public class KafkaDataOutputMainTest {
    KafkaDataOutputMain kafkaDataOutputMain=new KafkaDataOutputMain();

    @Test
    public void testKafkaDataOutput() throws Exception {
        kafkaDataOutputMain.TestKafkaDataOutput("/user/root/wordcount/input/resultTweets/resultTweets.txt");
    }

}
