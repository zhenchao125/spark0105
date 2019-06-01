package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("word", "hello", "hello", "atguigu", "hello", "atguigu"))
        val wordOne: RDD[(String, Int)] = rdd1.map((_, 1))
        wordOne.groupByKey()
        // 涉及到聚集的时候, 使用的是默认的分区器: HashPartitioner
        wordOne.reduceByKey(_ + _)
        sc.stop()
        
    }
}
