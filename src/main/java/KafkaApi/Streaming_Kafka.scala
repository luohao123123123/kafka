package KafkaApi

import Util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

import java.util.Collections
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.reflect.internal.util.Collections


/**
 *Streamin连接kafka，做持久化的wordCount
 */
object Streaming_Kafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new StreamingContext(conf,Seconds(2))
    sc.checkpoint("E:\\kafka\\checkpoint")

    //获取连接Kafka的参数
    val kafkaParams = KafkaUtil.getStreaming_KafkaConsumer
    //Topic
    val topics = Array("topic01")


    //2.使用KafkaUtil连接Kafak获取数据
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](sc,
      LocationStrategies.PreferConsistent,//位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
      ConsumerStrategies.Subscribe[String, String](topics,kafkaParams))//消费策略,源码强烈推荐使用该策略

    val Data: DStream[String] = recordDStream.map(x=>x.value())
    Data.map((_,1))
      .updateStateByKey(
        (seq:Seq[Int],option:Option[Int])=>{
          val seqSum=seq.sum
         val optionSum=option.getOrElse(0)
        Some(seqSum+optionSum)
      })
      .print()

    sc.start()//开启
    sc.awaitTermination()//等待优雅停止
  }
}
