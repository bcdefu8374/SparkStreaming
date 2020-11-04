package Sparkstreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object SparkStreaming09_reduceByKeyAndWindow_reduce {
  def main(args: Array[String]): Unit = {
    // 1 初始化SparkStreamingContext
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 保存数据到检查点
    ssc.checkpoint("./ck")

    //2 通过监控端口创建DStream
    val lines = ssc.socketTextStream("hadoop102",9999)

    //3 切割+变换
    val wordToOne = lines.flatMap(_.split(" ")).map((_,1))

    //4.窗口
    val wordToSum: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (a: Int, b: Int) => (a + b),
      (x: Int, y: Int) => (x - y),
      Seconds(12),
      Seconds(6),
      new HashPartitioner(2),
      (x: (String, Int)) => x._2 > 0
    )
    wordToSum.print()

    // 6 启动=》阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
