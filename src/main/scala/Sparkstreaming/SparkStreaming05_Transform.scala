package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    //1.创建Sparkconf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    //2.初始化StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //在Driver端执行

    //4.转化rdd操作
    val woedToSumDStream = lineDStream.transform(
      rdd => {
        rdd.flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)
      }
    )
    woedToSumDStream.print()

    //3.开启和阻塞
    ssc.start()
    ssc.awaitTermination()

  }

}
