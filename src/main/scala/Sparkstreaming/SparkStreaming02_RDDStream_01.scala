package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author chen
 * @topic
 * @create 2020-11-01
 */
object SparkStreaming02_RDDStream_01 {
  def main(args: Array[String]): Unit = {
    //1.初始化SparkConf的配置
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //2.初始化StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.创建队列
    val rddqueue = new mutable.Queue[RDD[Int]]()

    //4.向队列中存放数据
    for (i <- 1 to 5)  { //一共创建5个队列
      rddqueue += ssc.sparkContext.makeRDD(1 to 2)
      Thread.sleep(2000)
    }

    //取出队列中的数据
    val inputDStream = ssc.queueStream(rddqueue,oneAtATime = false)

    //计算
    inputDStream.reduce(_+_).print()

    //5.启动和阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
