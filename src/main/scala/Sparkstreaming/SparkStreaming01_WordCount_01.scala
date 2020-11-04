package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic  每一批次过来的数据，互相之间隔离不联系
 * @create 2020-11-01
 */
object SparkStreaming01_WordCount_01 {
  def main(args: Array[String]): Unit = {
    //1.初始化sparkConf配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    //2.初始化StreamingContext配置
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //3.通过监控端口创建DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //4.处理单词的个数
    lines.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .print()

    //3.启动和阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
