package Sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object SparkStreaming01_WorkCount {
  def main(args: Array[String]): Unit = {

    //1.初始化spark配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming_wordcount");

    //2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.通过监控端口创建DStream，读进来数据为一行行
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //3.1 将每一行数据进行切分
    val wordToOneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    //3.2 打印
    wordToOneDStream.print()

    //4.启动SparkStreamingContext
    ssc.start()
    //将主线阻塞，主线程不退出
    ssc.awaitTermination()

  }

}
