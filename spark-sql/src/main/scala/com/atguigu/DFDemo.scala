package com.atguigu

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-06-04 16:37
  */
object DFDemo {
    def main(args: Array[String]): Unit = {
        // 1. 创建 SparkSession
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("DFDemo")
            .getOrCreate()
    
        // 2.  得到DF或者DS
        val df = spark.read.json("C:\\Users\\lzc\\Desktop\\class_code\\2019_01_05\\02_spark\\spark0105\\spark-sql\\src\\main\\resources\\json")
        df.createOrReplaceTempView("user")
        //3.  执行sql
        spark.sql("select * from user").show()
    
        spark.sql("insert into user values(20, 'zs')").show
        spark.stop()
    }
}
