package com.atguigu.day05

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-06-04 10:42
  */
object AccDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
//        val acc = new IntAcc
        val acc = new MapAcc
        sc.register(acc, "first")  // 累加器要注册.
        rdd1.foreach(x => {
            acc.add(x)
            println(x)
        })
        println("-----------------")
        println(acc.value)
        sc.stop()
    }
}
