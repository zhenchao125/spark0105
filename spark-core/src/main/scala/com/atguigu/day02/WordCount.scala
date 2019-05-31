package com.atguigu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-05-31 14:01
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        // 创建sc需要的配置
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        // 1. 先创建 SparkContext对象 sc
        val sc = new SparkContext(conf)
        // 2. 通过 sc 来得到 RDD
        val linesRDD: RDD[String] = sc.textFile("hdfs://hadoop201:9000/input")
        // 3. 对RDD做各种转换
        val wordRdd: RDD[String] = linesRDD.flatMap(_.split("\\W+"))
        val wordCountRDD: RDD[(String, Int)] = wordRdd.map((_, 1)).reduceByKey(_ + _)
        
        // 4. 对 RDD 做行动
        val result: Array[(String, Int)] = wordCountRDD.collect
        println(result.mkString(", "))
        // 5. 关闭sc
        sc.stop()
    }
}
