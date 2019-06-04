package com.atguigu.day05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        
//        val bc= sc.broadcast(List(10,20,30,40))
        val bc= List(10,20,30,40)
        rdd1.foreach(x => {
//            val list: List[Int] = bc.value
//            println(System.identityHashCode(list))
            println(System.identityHashCode(bc))
        })
        
        sc.stop()
        
    }
}
