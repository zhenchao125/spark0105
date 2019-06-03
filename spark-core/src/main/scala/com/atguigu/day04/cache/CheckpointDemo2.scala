package com.atguigu.day04.cache

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-06-03 14:18
  */
object CheckpointDemo2 {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        //        sc.setCheckpointDir("./ck1")
        sc.setCheckpointDir("hdfs://hadoop201:9000/ck0105")
        val rdd1 = sc.parallelize(Array(30, 50))
        val rdd3 = rdd1.map(x => {
            val y = x + ": " + System.currentTimeMillis()
            println(y)
            y
        })
        
        rdd3.checkpoint() // 碰到checkpoint并没有真正的去向磁盘写数据, 当第一个job执行完毕之后, 才开始从新计算数据,然后写入到磁盘
        rdd3.cache()
        
        rdd3.collect
        println("--------")
        rdd3.collect.foreach(println)
        //
//        Thread.sleep(10000000)
        sc.stop()
    }
}
