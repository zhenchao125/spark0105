package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-06-05 18:31
  */
object WordCountWidown {
    def main(args: Array[String]): Unit = {
        //1. 创建 StreamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(2))
        //2. 获取DStream
        val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 20000)
        //3. 对DStream进行处理
        val wordOne: DStream[(String, Int)] = lineStream.flatMap(_.split("\\W+")).map((_, 1))
        val result = wordOne.reduceByKeyAndWindow((_:Int) + (_:Int), Seconds(6), Seconds(4))
        // 4. 显示处理的结果
        result.print
        // 5. 启动ssc
        ssc.start()
        // 6. 让应用一直不停
        ssc.awaitTermination()
    }
}
