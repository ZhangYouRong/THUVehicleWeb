package com.tsinghua.kafka;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NettyProducer
{
    public static void main(String[] args) throws ExecutionException,InterruptedException
    {
        Properties props = new Properties();
        //kafka 集群，broker-list
        props.put("bootstrap.servers", "node1:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);



        for (int i = 0; i < 100; i++)
        {
            String data= i+",'张有容','123456','男','教师','www.bbk.com','XX大学','2016-08-12 14:43:26','备注'";
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), data));
        }
        System.out.println("成功发送100条消息至kafka，生产者关闭");
        producer.close();
    }
}

