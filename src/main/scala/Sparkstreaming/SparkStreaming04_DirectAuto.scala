package Sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object SparkStreaming04_DirectAuto {
  def main(args: Array[String]): Unit = {

    //1. 初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    //2.初始化SparkContext配置信息
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //2.1定义kafka参数：kafka集群地址，消费者组名称，key序列化，value序列化
    val kafkaPara: Map[String,Object] = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguguGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //3.获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //优先位置
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaPara) //消费策略：（订阅多个主题，配置参数）
    )
    //4.将每条消息的kv取出,输入是kv类型的，要取出value，否则类型不匹配
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

    //5.计算WordCount
    valueDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()


  }
}
