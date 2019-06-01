package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionByDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 7, 6, 1, 20), 2)
        
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
        
        val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new HashPartitioner(3))
        val rdd4: RDD[(Int, Int)] = rdd3.partitionBy(new HashPartitioner(3))
    
        val rdd5: RDD[(Int, (Int, Int))] = rdd3.mapPartitionsWithIndex {
            case (index, it) => it.map((index, _))
        }
        rdd5.collect.foreach(println)
        sc.stop()
        
        
    }
}
