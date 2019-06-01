package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-06-01 14:27
  */
object FoldByKeyDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        // reduceByKey的升级版本
        val rdd2: RDD[(String, Int)] = rdd1.foldByKey(0)(_ + _)
        
        rdd2.collect.foreach(println)
        sc.stop()
    }
}
