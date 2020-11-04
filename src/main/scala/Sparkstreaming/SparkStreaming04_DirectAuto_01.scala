package Sparkstreaming


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-01
 */
object SparkStreaming04_DirectAuto_01 {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //初始化StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //3.定义kafka参数，kafka的集群地址，消费者组名称，key序列化，value序列化
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "bbbb"
    )

    //4.读取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaParams) //订阅主题
    )

    //5. 将每一条key value取出来
    val valueDStream = kafkaDStream.map(record => record.value())

    //6.计算wordcount
    valueDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()


    //启动和阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
