package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark的配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")

    //2.初始化SparkContext配置信息
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(4))

    //3.创建rdd队列
    val rddqueue = new mutable.Queue[RDD[Int]]()

    //4 创建QueueInputDStream
    val inputDStream = ssc.queueStream(rddqueue,oneAtATime = false)

    //5.处理对列中的rdd
    val sumDStream: DStream[Int] = inputDStream.reduce(_+_)

    //6.打印数据
    sumDStream.print()


    //启动任务
    ssc.start()

    //8.循环创建并向RDD对列中放入RDD
    for(i <- 1 to 5) {
      rddqueue += ssc.sparkContext.makeRDD(1 to 2)
      Thread.sleep(2000)
    }

    //关闭
    //ssc.awaitTermination()
    ssc.stop()
  }
}
