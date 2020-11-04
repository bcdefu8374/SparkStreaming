package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-02
 */
object SparkStreaming07_window_02 {
  def main(args: Array[String]): Unit = {
    //1.初始化Sparkconf
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //2.初始化StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //设置检查点,存放以前的数据
    ssc.checkpoint("./ck")

    //3.创建DStream，读进去的数据为一行行
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val wordToOne = lines.flatMap(_.split(" ")).map((_,1))

    val wordToWindow: DStream[(String, Int)] = wordToOne.window(Seconds(12),Seconds(6))

    wordToWindow.reduceByKey(_+_).print()



    //3.启动+阻塞
    ssc.start()
    ssc.awaitTermination()
  }

}
