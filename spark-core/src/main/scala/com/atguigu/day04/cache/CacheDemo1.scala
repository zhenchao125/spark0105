package com.atguigu.day04.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        val rdd2: RDD[Int] = rdd1.map(x => {
            println("map1:" + x)
            x + 1
        })
        val rdd3 = rdd2.map(x => {
            println("map2:" + x)
            x + 1
        })
        // 只缓存到内存中,
        rdd3.cache()
//        rdd3.persist(StorageLevel.DISK_ONLY)
        rdd3.collect.foreach(print)
        println("--------")
        rdd3.collect.foreach(print)
        
        Thread.sleep(1000000)
        sc.stop()
        
    }
}
