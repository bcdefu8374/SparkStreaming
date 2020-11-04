package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-01
 */
object SparkStreaming05_Transform_01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //3.创建DStream
    val lineDStream = ssc.socketTextStream("hadoop102",9999)

    // 在Driver端执行，全局一次
    println("111111111:" + Thread.currentThread().getName)

    //4.使用transform进行转化
    lineDStream.transform(
      rdd => {
        // 在Driver端执行(ctrl+n JobGenerator)，一个批次一次
        println("222222:" + Thread.currentThread().getName)

        rdd.flatMap(_.split(" "))
          .map( x => {
            // 在Executor端执行，和单词个数相同
            println("333333:" + Thread.currentThread().getName)
            (x,1)
          })
          .reduceByKey(_+_)
      }
    ).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
