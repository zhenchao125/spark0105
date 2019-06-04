package com.atguigu.day05

import org.apache.spark.util.AccumulatorV2

/*
value应该是一个map, 然后在map中提供3个键值对: sum: 10, count: 2, avg: 5
 */
class MapAcc extends AccumulatorV2[Int, Map[String, Double]] {
    var map = Map[String, Double]()
    
    override def isZero: Boolean = map.isEmpty
    
    override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
        val acc = new MapAcc
        acc.map ++= map
        acc
    }
    
    override def reset(): Unit = {
        map = Map[String, Double]()
    }
    
    override def add(v: Int): Unit = {
        // sum 和 count
        map += "sum" -> (map.getOrElse("sum", 0d) + v)
        map += "count" -> (map.getOrElse("count", 0d) + 1)
    }
    
    override def merge(other: AccumulatorV2[Int, Map[String, Double]]): Unit = {
        other match {
            case o: MapAcc =>
                map += ("sum" -> (map.getOrElse("sum", 0d) + o.map.getOrElse("sum", 0d)))
                map += "count" -> (map.getOrElse("count", 0d) + o.map.getOrElse("count", 0d))
            case _ =>
        }
    }
    
    override def value: Map[String, Double] = {
        map += "avg" -> (map.getOrElse("sum", 0d) / map.getOrElse("count", 1d))
        // 先计算平均值
        map
    }
}
