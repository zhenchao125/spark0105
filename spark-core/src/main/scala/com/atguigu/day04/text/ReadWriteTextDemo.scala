package com.atguigu.day04.text

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadWriteTextDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd: RDD[String] = sc.textFile("c:/input")
        println(rdd.partitions.length)
        rdd.collect
//        val rdd = sc.parallelize(List(30, 50, 70, 60, 10, 20))
//        rdd.saveAsTextFile("./a.txt")
        sc.stop()
        
    }
}
