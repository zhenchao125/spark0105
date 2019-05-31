package com.atguigu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-05-31 15:38
  */
object MakeRDDDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("MakeRDDDemo")
        
        val sc = new SparkContext(conf)
        
        val rdd1: RDD[Int] = sc.parallelize(Array(10, 20, 30, 40, 50, 60, 70))
        println(rdd1.partitions.length)
        rdd1.collect.foreach(println)
        
        sc.stop()
    }
}
