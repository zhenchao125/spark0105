package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-06-01 09:05
  */
object CoalesceDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20), 4)
        println(rdd1.partitions.length)
        // 只能减少分区, 不能增加分区
        val rdd2: RDD[Int] = rdd1.coalesce(2)
        
        println(rdd2.partitions.length)
        sc.stop()
        
    }
}
