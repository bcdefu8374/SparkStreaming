package Sparkstreaming


import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


/**
 * @author chen
 * @topic
 * @create 2020-11-02
 */
object SparkStreaming11_stop {
  def main(args: Array[String]): Unit = {
    //初始化配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //监控端口
   val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //4 设置优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")

    //切割和转换
    lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //5.开启监控程序
    new Thread(new MonitorStop(ssc)).start()

    //开启和阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}


class MonitorStop(ssc: StreamingContext) extends Runnable{
  override def run(): Unit = {
    //获取hdfs的文件系统
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"),new Configuration(),"atguigu")

    //判断
    while (true){
      Thread.sleep(5000)
      //获取/stopSpark路径存在
      val result: Boolean = fs.exists(new Path("hdfs://hadoop102:8020/stopSpark"))
      if (result) {
        val state: StreamingContextState = ssc.getState()

        if (state == StreamingContextState.ACTIVE) {
          //优雅关闭
          ssc.stop(stopSparkContext = true,stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}