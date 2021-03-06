package com.atguigu.day04.funpass

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FunPassDemo2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("SerDemo")
            .setMaster("local[*]")
            // org.apache.spark.serializer.KryoSerializer
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[Searcher1]))
        
        val sc = new SparkContext(conf)
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
        
        val searcher = new Searcher1("hello")
        val rdd2: RDD[String] = searcher.getMatchedRDD3(rdd)
        rdd2.collect.foreach(println)
        
        sc.stop()
    }
}
// 在 rdd 中, 查找包含这个字符串的元素 RDD: "hello", "how"   he
case class Searcher1(val query: String)  {
    // 判断 s 中是否包括子字符串 query
    def isMatch(s : String) ={
        val q = query
        s.contains(q)
    }
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) ={
        rdd.filter(isMatch)  //
    }
    
    def getMatchedRDD3(rdd: RDD[String]) ={
        val is = isMatch _
        rdd.filter(is)  //
    }
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD2(rdd: RDD[String]) ={
        val q1 = query
        rdd.filter(x => x.contains(q1))
    }
}

