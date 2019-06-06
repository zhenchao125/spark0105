package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-06-06 13:48
  */
object TransfromDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc = new StreamingContext(conf, Seconds(3))
        val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 20000)
        val result = dstream.transform(rdd => {
            rdd.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
        })
        result.print
        ssc.start()
        ssc.awaitTermination()
        
    }
}
