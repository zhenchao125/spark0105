package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-06-01 16:19
  */
object Pratice {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val path = ClassLoader.getSystemResource("agent.log").getPath
        val linesRDD: RDD[String] = sc.textFile(path)
        val proviceAdsOneRDD= linesRDD.map(line => {
            println("aaaa")
            val row: Array[String] = line.split(" ")
            ((row(1), row(4)), 1)
        })
        
        val reducedRDD: RDD[((String, String), Int)] = proviceAdsOneRDD.reduceByKey(_ + _)
        val proviceToAdsCountRDD: RDD[(String, (String, Int))] = reducedRDD.map {
            case ((provice, ads), count) => (provice, (ads, count))
        }
        val groupedRDD: RDD[(String, Iterable[(String, Int)])] = proviceToAdsCountRDD.groupByKey
        val resultRDD: RDD[(String, List[(String, Int)])] = groupedRDD.map {
            
            case (pro, it) => println("bbb"); (pro, it.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
        }.sortByKey()
        resultRDD.collect.foreach(println)
        sc.stop()
    }
}
/*
=> 	RDD[((pro, ads))]   map
=> 	RDD[((pro, ads), 1)]				ruduceByKey
=> 	RDD[((pro, ads), count)]		map
=>  RDD[(pro, (ads, count))]   groupByKey
RDD[(pro, List((ads, count), (ads, count)))]
 */
