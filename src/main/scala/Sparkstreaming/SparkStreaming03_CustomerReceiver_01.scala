package Sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic  自定义数据源
 * @create 2020-11-01
 */
object SparkStreaming03_CustomerReceiver_01 {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    //初始化Streamingcontext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //创建自定义的接收Streaming
    val lineDStream = ssc.receiverStream(new CustomerReceiver01("hadoop102",9999))

    //将获取到的数据进行wordcount
    lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //启动和阻塞
    ssc.start()
    ssc.awaitTermination()

  }

}

class CustomerReceiver01(host: String,port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  //最初启动的时候，调用该方法，作用为：读数据并将数据发送给spark
  override def onStart(): Unit = {
    new Thread("Sparkreceiver"){
      override def run(): Unit ={
        receive()
      }
    }
  }

  def receive(): Unit ={
    //创建一个socket
    val socket = new Socket(host,port)

    //创建一个BufferedReader用于获取数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

    //读取数据
    val input = reader.readLine()

    //判断读取数据，如果receive关闭或者输入数据不为空
    while (!isStopped() && input != null){
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
