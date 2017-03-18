package com.basic.kafka.producer;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 79875 on 2017/3/3.
 * 运行kafka Producer类
 * topic=tweetswordtopic
 * nohup java -cp stormTest-1.0.2-SNAPSHOT-jar-with-dependencies.jar com.basic.kafka.producer.MainSocketProducer tweetswordtopic3 10 5 10 >MainProducer.out &
 */
public class MainSocketProducer {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
           System.out.println("Please Input Topic ");
        }
        String topic = (String) args[0];
        Integer ProducerNum=Integer.valueOf(args[1]);
        Integer coreThreadNum=Integer.valueOf(args[2]);
        Integer maxThreadNum=Integer.valueOf(args[3]);
        System.out.println("Topic = " + topic);


//        corePoolSize
//        核心线程数，核心线程会一直存活，即使没有任务需要处理。当线程数小于核心线程数时，即使现有的线程空闲，线程池也会优先创建新线程来处理任务，而不是直接交给现有的线程处理。
//        核心线程在allowCoreThreadTimeout被设置为true时会超时退出，默认情况下不会退出。
//        maxPoolSize
//        当线程数大于或等于核心线程，且任务队列已满时，线程池会创建新的线程，直到线程数量达到maxPoolSize。如果线程数已等于maxPoolSize，且任务队列已满，则已超出线程池的处理能力，线程池会拒绝处理任务而抛出异常。
//        keepAliveTime
//        当线程空闲时间达到keepAliveTime，该线程会退出，直到线程数量等于corePoolSize。如果allowCoreThreadTimeout设置为true，则所有线程均会退出直到线程数量为0。
//        allowCoreThreadTimeout
//        是否允许核心线程空闲退出，默认值为false。
//        queueCapacity
//        任务队列容量。从maxPoolSize的描述上可以看出，任务队列的容量会影响到线程的变化，因此任务队列的长度也需要恰当的设置。
        ThreadPoolExecutor executor = new ThreadPoolExecutor(coreThreadNum, maxThreadNum, 200, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(10));

        KafkaTweetSocketProducer kafkaTweetProducer[] = new KafkaTweetSocketProducer[ProducerNum];
        for(int i=0;i<ProducerNum;i++){
            kafkaTweetProducer[i]=new KafkaTweetSocketProducer(topic);
//            kafkaTweetProducer.publishMessage(topic);
            executor.execute(kafkaTweetProducer[i]);
            System.out.println("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+
                    executor.getQueue().size()+"，已执行玩别的任务数目："+executor.getCompletedTaskCount());
        }
        executor.shutdown();
    }
    }
