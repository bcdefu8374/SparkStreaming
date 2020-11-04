package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object SparkStreaming08_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    //初始化SparkStreaming
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SprakStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    // 保存数据到检查点
    ssc.checkpoint("./ck")

    //通过监控端口创建DStream，读进去的数据为一行行
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //切割+变换
    val wordToOne = lines.flatMap(_.split(" ")).map((_,1))

    //reduce窗口
    val wordCounts = wordToOne.reduceByKeyAndWindow((a: Int,b: Int) => a+b,Seconds(12),Seconds(6))

    wordCounts.print()

    //启动和阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
