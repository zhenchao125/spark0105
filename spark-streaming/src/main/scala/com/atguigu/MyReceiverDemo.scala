package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-06-06 09:07
  */
object MyReceiverDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("MyReceiverDemo")
        val ssc = new StreamingContext(conf, Seconds(5))
        
        val stream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop201", 20000))
        
        stream.
            flatMap(_.split("\\W+")).
            map((_, 1)).
            reduceByKey(_ + _).
            print()
        
        ssc.start()
        ssc.awaitTermination()
    }
}
