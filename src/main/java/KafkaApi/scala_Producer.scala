package KafkaApi

import Util.KafkaUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object scala_Producer {
  def main(args: Array[String]): Unit = {
    val kafkaUtil = new KafkaUtil
    val producer: KafkaProducer[String, String] = kafkaUtil.getProducer
    for (i <- 0 until 100000000) {
      //            发送数据
      //            ("second",0，key,"");指定分区
      //            ("second",key,"");指定key，根据key分区
      //            ("second","");不指定，随机分区，轮询
      producer.send(new ProducerRecord[String, String]("topic01", "hello-" + i))
      println(i)
    }
    producer.close()
  }
}
