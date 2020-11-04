package Sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")

    //2.初始化sparkContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //3.创建自定义receiver的Streaming
    val lineDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102",9999))

    //4.计算wordcount
    val words: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //5.收集出来
    words.print()

    //8.启动
    ssc.start()
    ssc.awaitTermination()
  }

}


//自定义接收数据源
class CustomerReceiver(host: String ,port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //最初启动的时候，调用方法，线程池
  override def onStart(): Unit = {
    new Thread("Socket Receiver"){
      override def run(){
        receive()
      }
    }.start()
  }

  //读数据并将数据发送给spark
  def receive(): Unit = {
    //1.创建一个socket
    val socket = new Socket(host,port)

    //2.创建一个BufferedReader用来读取端口传入的数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))

    //3.读取数据
    val input: String = reader.readLine()

    //4.当receiver没有关闭并且输入不为空，则循环发送数据给socket
    while (!isStopped() && input != null) {
      store(input)
    }

    //如果循环结束就关闭资源
    reader.close()
    socket.close()

    //重启接收任务
    restart("restart")
  }

  override def onStop(): Unit = {}
}
