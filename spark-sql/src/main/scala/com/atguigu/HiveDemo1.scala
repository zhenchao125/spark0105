package com.atguigu

import org.apache.spark.sql.SparkSession

object HiveDemo1 {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            // 添加支持外置的 hive
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", "hdfs://hadoop201:9000/user/hive/warehouse")
            .getOrCreate()
        import spark.implicits._
        
        spark.sql("show tables").show
//        spark.sql("create table test(id int)").show
        spark.sql("create database spark0107").show
        spark.sql("use spark0107").show
        spark.sql("create table test(id int)").show
        spark.sql("insert into test values(1)").show
        spark.sql("insert into test values(2)").show
        
        spark.stop()
    }
}
