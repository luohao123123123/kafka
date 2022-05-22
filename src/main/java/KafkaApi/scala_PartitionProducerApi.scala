package KafkaApi

import Util.KafkaUtil
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}

/**
 * 生产者自定义分区
 * 需求：topic01创建了两个分区0和1，需要把0~100的数字放入topic01，如果这个数是偶数则放入分区1，如果不是偶数则放入分区0
 */
object scala_PartitionProducerApi {
  def main(args: Array[String]): Unit = {
    val props = KafkaUtil.getProducerProperties
    //添加自定义分区class
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "KafkaApi.CustomPartitioner")
    //获取KafkaProducer
    val producer = KafkaUtil.getProducer(props)
    for (i <- 0 until 100) {
      producer.send(new ProducerRecord[String, String]("topic01", i + "", i + ""))
    }
    producer.close()
  }
}
