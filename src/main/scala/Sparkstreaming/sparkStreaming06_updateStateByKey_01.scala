package Sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-01
 */
object sparkStreaming06_updateStateByKey_01 {

  //定义更新状态方法，参数seq为当前批次单词次数，state为以往彼此单词次数
  val updateFunc = (seq: Seq[Int],option: Option[Int]) => {
    val currentcount = seq.sum  //历史批次累加结果

    val previousCount = option.getOrElse(0)

    //总的数据累加
    Some(currentcount + previousCount)
  }

  def createSSC() : StreamingContext = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("./ck")
    val lines = ssc.socketTextStream("hadoop102",9999)

    val wordToOne = lines.flatMap(_.split(" ")).map((_,1))
    wordToOne.updateStateByKey[Int](updateFunc).print()
    ssc

  }
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate("./ck",() =>createSSC())
    ssc.start()
    ssc.awaitTermination()
  }
}
