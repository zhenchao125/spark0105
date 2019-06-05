package com.atguigu

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author lzc
  * Date 2019-06-05 10:50
  */
object JDBCDemo2 {
    
    
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        
       
        var props: Properties = new Properties()
        props.put("password", "aaa")
        props.put("user", "root")
        
        val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop201:3306/rdd", "user", props)
        
        df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop201:3306/rdd", "user", props)
        spark.stop()
    }
}

/*
通用:
    spark.read.format("jdbc").option()
 */