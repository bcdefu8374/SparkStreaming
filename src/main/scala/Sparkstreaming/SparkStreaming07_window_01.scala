package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-01
 */
object SparkStreaming07_window_01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.checkpoint("./ck")
    val lines = ssc.socketTextStream("hadoop102",9999)

    val wordToOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))

    val wordToOneWindow: DStream[(String, Int)] = wordToOne.window(Seconds(12),Seconds(6))

    wordToOneWindow.reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
