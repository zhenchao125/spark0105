package com.atguigu.day04.dep

import org.apache.spark.{SparkConf, SparkContext}

object DependencesDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        rdd1.map((_, 1))
            .reduceByKey(_ + _)
            .map(x => x._1)
            .groupBy(x => x)
            .map(x => (x._1, x._2.toList.length)).collect
        Thread.sleep(10000000)
        sc.stop()
        
    }
}
