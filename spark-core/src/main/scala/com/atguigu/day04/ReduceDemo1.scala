package com.atguigu.day04

import org.apache.spark.{SparkConf, SparkContext}

object ReduceDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(1, 2, 3, 4),2)
        println(rdd1.fold(1)(_ + _))
        
        //        println(rdd1.reduce(_ + _))
        //        println(rdd1.fold(0)(_ + _))
        //        val reuslt: Int = rdd1.aggregate(0)(_ + _, _ + _)
        
        //
//        val result = rdd1.aggregate(Int.MinValue)(_.max(_), _ + _) - Int.MinValue
    
//        val rdd1 = sc.makeRDD(Array("a", "b", "c", "d"), 3)
//        val result: String = rdd1.aggregate("x")(_ + _, _ + _)
        
//        println(result)
        //
//        rdd1.collect.foreach(x => println(x))
//        rdd1.foreach(x => println(x))
        sc.stop()
    }
}
