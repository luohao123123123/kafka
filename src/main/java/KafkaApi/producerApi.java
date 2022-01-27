package KafkaApi;

import Util.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class producerApi {
    public static void main(String[] args){
        KafkaUtil kafkaUtil=new KafkaUtil();
        KafkaProducer<String, String> producer = kafkaUtil.getProducer();
        for (int i=0;i<100000000;i++){
//            发送数据
//            ("second",0，key,"");指定分区
//            ("second",key,"");指定key，根据key分区
//            ("second","");不指定，随机分区，轮询
            producer.send(new ProducerRecord<>("topic01","hello"), (recordMetadata, e) -> {
                if(e==null){
                    System.out.println(recordMetadata.topic()+"-"+recordMetadata.partition()+"-"+recordMetadata.offset());
                }
            });
        }

        producer.close();
    }
}
