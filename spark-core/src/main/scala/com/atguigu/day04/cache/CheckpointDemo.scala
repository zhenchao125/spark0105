package com.atguigu.day04.cache

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Author lzc
  * Date 2019-06-03 14:18
  */
object CheckpointDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("./ck1")
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        val rdd2: RDD[Int] = rdd1.map(x => {
            println("map1:" + x)
            x + 1
        })
        val rdd3 = rdd2.map(x => {
            println("map2:" + x)
            x + 1
        })
        rdd3.cache()
        rdd3.checkpoint()// 碰到checkpoint并没有真正的去向磁盘写数据, 当第一个job执行完毕之后, 才开始从新计算数据,然后写入到磁盘
        rdd3.collect.foreach(print)
        println("--------")
        rdd3.collect.foreach(print)
//
        Thread.sleep(10000000)
        sc.stop()
    }
}
