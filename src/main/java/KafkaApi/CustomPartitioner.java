package KafkaApi;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;


/**
 * 生产者自定义分区
 *需求：topic01创建了两个分区0和1，需要把0~100的数字放入topic01，如果这个数是偶数则放入分区1，如果不是偶数则放入分区0
 */
public class CustomPartitioner implements Partitioner {

    public void configure(Map<String, ?> configs) {

    }

    //分区策略
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String Stringkey=(String) key;
        int intkey = Integer.parseInt(Stringkey);  //获取key，转为int类型
        if(intkey%2==0){   //如果这个数是偶数则放入分区1
            return 1;
        }
           return 0;   //否则则放入分区0
    }

    public void close() {
        //清理工作
    }
}
