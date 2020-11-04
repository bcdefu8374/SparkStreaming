package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-01
 */
object SparkStreaming08_reduceByKeyAndWindow_01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.checkpoint("./ck")

    val lines = ssc.socketTextStream("hadoop102",9999)
    val wordToOne = lines.flatMap(_.split(" ")).map((_,1))

    val wordTowindowSum: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow((a: Int, b: Int) => (a + b),Seconds(12),Seconds(6))
    wordTowindowSum.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
