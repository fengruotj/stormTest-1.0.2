package com.basic.kafka.util;

import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by 79875 on 2017/3/30.
 */
public class KafkaUtil {
    private  BufferedReader in=null;
    /**
     * Producer的两个泛型，第一个指定Key的类型，第二个指定value的类型
     */
    private Producer<String, String> producer;

    private static KafkaUtil kafkaUtil;

    private static KafkaUtil INSTANCE = new KafkaUtil();

    public static KafkaUtil getInstance(){
        return INSTANCE;
    }

    public KafkaUtil() {
        Properties props = new Properties();
        /**
         * 指定producer连接的broker列表
         */
        props.put("metadata.broker.list", "root8:9092 ,root9:9092 ,root10:9092");
        /**
         * 指定message的序列化方法，用户可以通过实现kafka.serializer.Encoder接口自定义该类
         * 默认情况下message的key和value都用相同的序列化，但是可以使用"key.serializer.class"指定key的序列化
         */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        /**
         * 这个参数用于通知broker接收到message后是否向producer发送确认信号
         *  0 - 表示producer不会等待broker发送ack
         *  1- 表示leader接受到消息后发送ack
         * -1 - 当所有的follower都同步消息后发生ack
         */
        props.put("request.required.acks", "0");
        /**
         * sync同步（默认）,async表示异步可以提高发送吞吐量
         */
        props.put("producer.type","async");
        /**
         * 在async模式下，当message缓存超时后，将会批量发送给broker，默认为5000ms
         */
        props.put("queue.buffering.max.ms","5000");
        /**
         * 在async模式下，Producer端允许buffer的最大信息量
         */
        props.put("queue.buffering.max.messages","100000");
        /**
         * 在async模式下，指定每次批量发送的数据量，默认为200
         */
        props.put("batch.num.messages","500");
        /**
         * kafka 消息压缩方式 这里采用snappy压缩方式
         */
        props.put("compression.type","snappy");
        ProducerConfig config = new ProducerConfig(props);

        producer = new KafkaProducer<String, String>(props);
    }

    /**
     * 根据topic和消息条数发送消息
     * @param topic
     */
    public void publishMessage(String topic,String key,String value) throws IOException {
        // 做key Producer默认让key的hashcode如Partitions取模
        producer.send(new ProducerRecord<String, String>(topic,key,value));
    }
}
