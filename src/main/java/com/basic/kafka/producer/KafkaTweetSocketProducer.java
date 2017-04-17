package com.basic.kafka.producer;

import com.basic.util.PropertiesUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Date;
import java.util.Properties;

/**
 * Created by 79875 on 2017/3/3./**
 * 一个简单的Kafka Producer类，传入两个参数：
 * topic num
 * 设置主题和message条数
 *
 * 执行过程：
 * 1、创建一个topic
 * kafka-topic.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic xxxx
 * 2、运行本类中的代码
 * 3、查看message
 * kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic xxxx
 * kafka
 */
public class KafkaTweetSocketProducer implements Runnable{

    /**
     * Producer的两个泛型，第一个指定Key的类型，第二个指定value的类型
     */
    private static Producer<String, String> producer;

    private String topic;

    private Socket sock=null;
    private BufferedReader in=null;

    public KafkaTweetSocketProducer(String topic) {
        this.topic=topic;

        /**
         * 加载数据源文件 tweetsResult.txt分词文件
         */
        if(sock==null){
            try {
                sock=new Socket(PropertiesUtil.getProperties("socketIPAddress"),Integer.valueOf(PropertiesUtil.getProperties("socketPort")));
                in= new BufferedReader(new InputStreamReader(sock.getInputStream()));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
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

        producer = new Producer<String, String>(config);
    }

//    public static void main(String[] args) {
//        if (args.length < 2) {
//            System.out.println("Please Input Topic and Message Numbers");
//        }
//        String topic = (String) args[0];
//        int count = Integer.parseInt((String) args[1]);
//        System.out.println("Topic = " + topic);
//        System.out.println("Message Nums = " + count);
//
//        SimpleKafkaProducer simpleProducer = new SimpleKafkaProducer();
//        simpleProducer.publishMessage(topic, count);
//    }

    /**
     * 根据topic和消息条数发送消息
     * @param topic
     */
    public void publishMessage(String topic) throws IOException {

        while(true) {
            String msg = "";
            try {
                msg = in.readLine();
            }catch (Exception e){
                e.printStackTrace();
                break;
            }
            if(msg==null){
                break;
            }
            producer.send(new KeyedMessage<String, String>(topic, msg));
            System.out.println("msg =" + msg +", msg pushed time"+new Date());
        }
        producer.close();
    }

    public void run() {
        try {
            publishMessage(topic);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
