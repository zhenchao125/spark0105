package com.atguigu

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-06-05 10:50
  */
object JDBCDemo {
    
    
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        
        // 1. 通用方式从jdbc中读取数据
       /* val df: DataFrame = spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://hadoop201:3306/rdd")
            .option("user", "root")
            .option("password", "aaa")
            .option("dbtable", "user")
            .load()
        
        df.createOrReplaceTempView("user")
        spark.sql("select * from user where id > 100").show*/
        
        // 2. 专用方式
        var props: Properties = new Properties()
        props.put("password", "aaa")
        props.put("user", "root")
        val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop201:3306/rdd", "user", props)
        df.show
        spark.stop()
    }
}
/*
通用:
    spark.read.format("jdbc").option()
 */