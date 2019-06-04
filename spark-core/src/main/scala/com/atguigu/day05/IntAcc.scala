package com.atguigu.day05

import org.apache.spark.util.AccumulatorV2

class IntAcc extends AccumulatorV2[Int, Int] {
    var sum = 0
    
    // 判断最后累加的值是不是 "0"
    override def isZero: Boolean = sum == 0
    
    // 如何copy累加器
    override def copy(): AccumulatorV2[Int, Int] = {
        val acc = new IntAcc
        acc.sum = sum
        acc
    }
    
    // 重置
    override def reset(): Unit = {
        sum = 0
    }
    
    // 累加
    override def add(v: Int): Unit = {
        sum += v
    }
    
    // 累加器值的合并
    override def merge(other: AccumulatorV2[Int, Int]): Unit = {
        other match {
            case o: IntAcc => this.sum += o.sum
            case _ =>
        }
    }
    
    // 返回最终值
    override def value: Int = sum
}
