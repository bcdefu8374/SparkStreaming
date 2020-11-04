package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {
    //1.初始化Sparkconf
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //2.初始化StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint("./ck")
    //3.创建DStream，读进去的数据为一行行
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //4.切割+变换
    val wordToOneDStream = lines.flatMap(_.split(" ")).map((_,1))

    //5.获取窗口返回数据
    val wordToOneByWindow: DStream[(String, Int)] = wordToOneDStream.window(Seconds(12),Seconds(6))

    //6.聚合窗口数据并打印
    wordToOneByWindow.reduceByKey(_+_).print()
    //3.启动+阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
