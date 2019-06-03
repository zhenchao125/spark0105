package com.atguigu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20, 30, 40, 50, 2, 1,3), 2)
//        rdd1.map(x => {println(x); (x, 1)}).sortByKey().collect
        val rdd2= rdd1.map(x => {println(x); x}).zipWithIndex().take(10)
        Thread.sleep(10000000)
        sc.stop()
        
    }
}
