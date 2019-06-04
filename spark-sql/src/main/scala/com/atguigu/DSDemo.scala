package com.atguigu

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019-06-04 16:37
  */
object DSDemo {
    def main(args: Array[String]): Unit = {
        // 1. 创建 SparkSession
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("DFDemo")
            .getOrCreate()
        import spark.implicits._
        
        // 2.  得到DF或者DS
        val df = spark.read.json("C:\\Users\\lzc\\Desktop\\class_code\\2019_01_05\\02_spark\\spark0105\\spark-sql\\src\\main\\resources\\user.json")
        val ds: Dataset[User] = df.as[User]
        //        ds.show
        //        ds.createOrReplaceTempView("user")
        //        spark.sql("select * from user").show()
        
        spark.stop()
    }
}

case class User(age: BigInt, name: String)