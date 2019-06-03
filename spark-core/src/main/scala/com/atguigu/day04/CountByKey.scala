package com.atguigu.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-06-03 09:35
  */
object CountByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20, 10, 20, 40, 70))
        println(rdd1.map((_, 1)).countByKey())
        sc.stop()
    }
}
/*
reduceByKey和countByKey的区别

 */