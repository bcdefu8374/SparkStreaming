package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-10-31
 */
object sparkStreaming06_updateStateByKey {

  //定义更状态方法，参数seq为当前批次单词次数，state为以往批次单词次数
  val updateFunc = (seq: Seq[Int],state: Option[Int]) => {
    //当前批次累加
    val currentCount: Int = seq.sum

    //历史批次累加结果,如果历史批次有，我就累加，否则累加为0
    val previousCount: Int = state.getOrElse(0)

    //总的数据累加
    Some(currentCount + previousCount)
  }

  def createSCC(): StreamingContext = {
      //1.创建Sparkconf
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      //2.创建Streamingcontext
      val ssc = new StreamingContext(sparkConf,Seconds(3))

      ssc.checkpoint("./ck")

      //3.创建DStream
      val lines = ssc.socketTextStream("hadoop102",9999)

      //3.1 统计单词，求wordcount
      lines.flatMap(_.split(" "))
        .map((_,1))
        .updateStateByKey[Int](updateFunc)
        .print()
      ssc
  }

    def main(args: Array[String]): Unit = {

      val ssc = StreamingContext.getActiveOrCreate("./ck",() =>createSCC())
      //4.开始和阻塞
      ssc.start()
      ssc.awaitTermination()
  }
}
