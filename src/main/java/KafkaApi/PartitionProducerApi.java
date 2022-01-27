package KafkaApi;

import Util.KafkaUtil;
import org.apache.kafka.clients.producer.*;
import java.util.Properties;

/**
 * 生产者自定义分区
 *需求：topic01创建了两个分区0和1，需要把0~100的数字放入topic01，如果这个数是偶数则放入分区1，如果不是偶数则放入分区0
 */
public class PartitionProducerApi {
    public static void main(String[] args) {
        KafkaUtil kafkaUtil=new KafkaUtil();
        //获取kafka配置信息
        Properties props = kafkaUtil.getProducerProperties();
        //添加自定义分区class
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"KafkaApi.CustomPartitioner");

        //获取KafkaProducer
        KafkaProducer<String, String> producer = kafkaUtil.getProducer(props);
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>("topic01", i + "", i + ""), (recordMetadata, e) -> {  //把0~100作为key，同时也作为value
                    if (e == null) {
                        //观察数据的分区情况
                        System.out.println(recordMetadata.partition() + "-" + recordMetadata.offset());
                    }

                });
            }
        producer.close();
    }
}
