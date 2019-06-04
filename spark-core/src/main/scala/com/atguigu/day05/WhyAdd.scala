package com.atguigu.day05

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-06-04 10:42
  */
object WhyAdd {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        
        var a = 1
        rdd1.foreach(x => {
            a += 1
            println(x)
        })
        println("-----------------")
        println(a)
        sc.stop()
    }
}
