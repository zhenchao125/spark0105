package com.atguigu.day05

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-06-04 09:14
  */
object HdfsDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        sc.stop()
        
    }
}
