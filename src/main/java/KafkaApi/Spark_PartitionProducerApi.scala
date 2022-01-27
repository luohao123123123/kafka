package KafkaApi

import Util.KafkaUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1.Spark读取数据放入Kafka
 * 2.生产者自定义分区
 * 需求：topic01创建了两个分区0和1，需要把0~100的数字放入topic01，如果这个数是偶数则放入分区1，如果不是偶数则放入分区0
 */
object Spark_PartitionProducerApi {
  def main(args: Array[String]): Unit = {
    //todo:Spark连接
    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local")

    val sc=new SparkContext(conf)

    val array=Array(1,2,3,4,5,6,7,8,9,10)

    sc.makeRDD(array)
      .foreachPartition(x=>{
        //todo:Kafka连接
        val kafkaUtil = new KafkaUtil
        val props = kafkaUtil.getProducerProperties
        //添加自定义分区class
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "KafkaApi.CustomPartitioner")
        //获取KafkaProducer
        val producer  = kafkaUtil.getProducer(props)
        producer.send(new ProducerRecord[String, String]("topic01", x.toString, x.toString))
        producer.close()
    })
    sc.stop()
  }
}
