package cn.itcast.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream


object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("streaming word count").setMaster("local[6]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(
      hostname = "192.168.174.100",
      port = 9999,
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER
    )
    val words = lines.flatMap(_.split(" "))
    val tuples = words.map((_,1))
    val counts = tuples.reduceByKey(_+_)
    counts.print()
    ssc.start()
    ssc.awaitTermination()

//    if (args.length < 2) {
//      System.err.println("Usage: NetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }
//
//    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(sparkConf, Seconds(1))
//
//    val lines = ssc.socketTextStream(
//      hostname = args(0),
//      port = args(1).toInt,
//      storageLevel = StorageLevel.MEMORY_AND_DISK_SER)
//
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//
//    wordCounts.print()
//
//    ssc.start()
//    ssc.awaitTermination()
  }
}

  //    初始化环境
  //    数据的处理
  //    句子拆为单词
  //    转为单词
  //    词频reduce
  //    展示和启动

