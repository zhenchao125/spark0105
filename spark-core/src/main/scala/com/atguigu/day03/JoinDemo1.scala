package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "f"), (4, "e")))
        var rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))
//        val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
//        var rdd3 = rdd1.leftOuterJoin(rdd2)  // 4,("f", )
//        var rdd3 = rdd1.rightOuterJoin(rdd2)
        val rdd3 = rdd1.fullOuterJoin(rdd2)
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
/*
leftOuterJoin
rightOuterJoin
fullOuterJoin

 */