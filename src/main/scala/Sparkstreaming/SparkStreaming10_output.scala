package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-02
 */
object SparkStreaming10_output {
  def main(args: Array[String]): Unit = {
    //初始化配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //2.通过端口号创建DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //切割+变换
    val wordToOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))

    //输出
    wordToOne.foreachRDD(
      rdd => {
        //在Driver端执行，一个批次一次
        //在JobScheduler中查找(ctrl + f)streaming-job-executor
        println("222222:" + Thread.currentThread().getName)

        rdd.foreachPartition(
          //5.1 测试代码
          iter => iter.foreach(println)

          //5.2 企业代码
          //5.2.1 获取连接
          //5.2.2 操作数据
          //5.2.3 关闭连接
        )
      }
    )

    //开启和阻塞
    ssc.start()
    ssc.awaitTermination()
  }

}
