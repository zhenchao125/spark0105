package com.atguigu.day03

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        //        val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(0)( _ + _,  _ + _)
        // 按照key计算每个分区最大值的和
        //        val rdd2 = rdd1.aggregateByKey(Int.MinValue)((u, v) => u.max(v), _ + _)
        
        /*val rdd2 = rdd1.aggregateByKey((Int.MinValue, Int.MaxValue)) ({
            case ((max, min), v) => (max.max(v), min.min(v))
        } , {
            case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
        })*/
        
        /*val rdd2 = rdd1.aggregateByKey((0, 0))({
            case ((sum, count), v) => (sum + v, count + 1)
        } , {
            case ((sum1,count1), (sum2, count2)) => ((sum1 + sum2), (count1 + count2))
        }).map{
            case (key, (sum, count)) => (key, sum.toDouble / count)
        }*/
        
        /*val rdd2 = rdd1.combineByKey(
            v => v,
            (c: Int, v: Int) => c + v,
            (c1: Int, c2: Int) => c1 + c2
        )*/
        
        /*val rdd2 = rdd1.combineByKey(
            v => v,
            (c: Int, v: Int) => c.max(v),
            (c1:Int, c2:Int) => c1 + c2
        )*/
        
       /* val rdd2 = rdd1.sortByKey(false)*/
        
        var rdd2 = rdd1.mapValues(v => v * v)
        
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
