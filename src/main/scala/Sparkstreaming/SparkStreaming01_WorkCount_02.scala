package Sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-02
 */
object SparkStreaming01_WorkCount_02 {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    //初始化StreamingContext配置信息
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //监控端口
    val lines = ssc.socketTextStream("hadoop102",9999)

    //wordcount统计
    lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //启动和阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
