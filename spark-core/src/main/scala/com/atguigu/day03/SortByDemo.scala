package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object SortByDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(new User(10, "a"), new User(20, "b"), new User(20, "a"), new User(15, "c")))
        val sortedRdd: RDD[User] =
            rdd1.sortBy(user => (user.age, user.name))(Ordering.Tuple2(Ordering.Int, Ordering.String.reverse), ClassTag(classOf[(Int, String)]))
        sortedRdd.collect.foreach(println)
        
        sc.stop()
    }
}

class User(val age: Int, val name: String) extends Serializable {
    
    override def toString = s"User($age, $name)"
}
