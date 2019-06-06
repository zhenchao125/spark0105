package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck3")
        val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 20000)
        val wordOne: DStream[(String, Int)] = dstream.flatMap(_.split("\\W+")).map((_, 1))
        // 状态函数
        val result: DStream[(String, Int)] = wordOne.updateStateByKey {
            // seq: 表示本次的同一个key的所有的value组成的序列
            // option: 上一次同一个key对应的值
            case (seq, option) =>
                Some(seq.sum + option.getOrElse(0))
        }
        result.print
        ssc.start()
        ssc.awaitTermination()
        
    }
}
