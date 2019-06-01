package com.atguigu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 5, 7, 60, 1, 20))
        
        /*val result: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex {
            case (index, it) => it.map(x => (index, x))
        }*/
        
        /*val result: RDD[Int] = rdd1.flatMap(x => Array(x, x * x, x * x * x))*/
        /*val result: RDD[Array[Int]] = rdd1.glom()
        result.collect.foreach(a => println(a.mkString(", ")))  */
        
        // 慎用
        val result: RDD[(Boolean, Iterable[Int])] = rdd1.groupBy( x => x % 2 == 1)
        
        result.collect.foreach(println)
        sc.stop()
        
        
    }
}
