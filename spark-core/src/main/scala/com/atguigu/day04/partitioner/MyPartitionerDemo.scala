package com.atguigu.day04.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.util.Random

/**
  * Author lzc
  * Date 2019-06-03 15:05
  */
object MyPartitionerDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
        val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new MyPartitioner(2))
    
        val rdd4: RDD[Int] = rdd3.mapPartitionsWithIndex {
            case (index, it) => it.map(x => index)
        }
        rdd4.collect.foreach(println)
        sc.stop()
        
        // 如何打乱集合中的元素
    }
}
class MyPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    
    override def getPartition(key: Any): Int = {
        new Random().nextInt(partitions)
        // 0
    }
    
    override def hashCode(): Int = 1
    
    override def equals(obj: scala.Any): Boolean = true
}


/*
Partitioner:
    RDD[(key, value)] 才有可能分区器

 */