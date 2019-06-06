package com.atguigu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQueue {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(5))
        
        val rddQueue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
        
        val dstream: InputDStream[Int] = ssc.queueStream(rddQueue, true)
        dstream.reduce(_ + _).print
        
        ssc.start()
        val sc: SparkContext = ssc.sparkContext
        while (true) {
            val rdd: RDD[Int] = sc.parallelize(1 to 100)
            rddQueue += rdd
            Thread.sleep(2000)
        }
        ssc.awaitTermination()
    }
}
