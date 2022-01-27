package KafkaApi

import Util.KafkaUtil
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util.Collections

object scala_Consumer {
  def main(args: Array[String]): Unit = { //创建kafka连接
    val kafkaUtil = new KafkaUtil
    val consumer = kafkaUtil.getConsumer
    // 消费者订阅的topic, 可同时订阅多个
    consumer.subscribe(Collections.singletonList("topic01"))
//    consumer.assign(Collections.singletonList(new TopicPartition("topic01",1)));

    while (true) { // 读取数据，读取超时时间为100ms
      val records = consumer.poll(Duration.ofMillis(100))
      import scala.collection.JavaConversions._
      for (record <- records) {
        System.out.println("主题：" + record.topic + ",分区：" + record.partition + ",offset:" + record.offset + ",value:" + record.value)
      }
    }
  }
}
