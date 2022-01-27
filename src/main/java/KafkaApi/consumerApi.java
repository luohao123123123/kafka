package KafkaApi;

import Util.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class consumerApi {
    public static void main(String[] args) {
        //创建kafka连接
       KafkaUtil kafkaUtil = new KafkaUtil();
       KafkaConsumer<String, String> consumer = kafkaUtil.getConsumer();


        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Collections.singletonList("topic01"));
        //读取指定Topic的某个分区数据
        consumer.assign(Collections.singletonList(new TopicPartition("topic01",1)));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("主题：" + record.topic() + ",分区：" + record.partition() + ",offset:" + record.offset() + ",value:" + record.value());
            }
        }
    }
}
