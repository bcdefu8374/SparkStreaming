package Sparkstreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-01
 */
object SparkStreaming09_reduceByKeyAndWindow_reduce_01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("./ck")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val wordToOneCount = lines.flatMap(_.split(" ")).map((_,1))
    val wordToSum: DStream[(String, Int)] = wordToOneCount.reduceByKeyAndWindow(
      (a: Int, b: Int) => (a + b),
      (x: Int, y: Int) => (x - y),
      Seconds(12),
      Seconds(6),
      //为0就不打印了
      new HashPartitioner(2),
      (x: (String, Int)) => x._2 > 0
    )

    wordToSum.print()



    ssc.start()
    ssc.awaitTermination()
  }
}
