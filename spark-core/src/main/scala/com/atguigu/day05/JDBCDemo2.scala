package com.atguigu.day05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

object JDBCDemo2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop201:3306/rdd"
        val userName = "root"
        val passWd = "aaa"
        
        
        rdd1.foreachPartition(iterator => {
            // 建立连接
            Class.forName(driver)
            val conn: Connection = DriverManager.getConnection(url, userName, passWd)
            
            // 插入数据
            iterator.foreach(x => {
                val sql = "insert into user values(?)"
                val ps: PreparedStatement = conn.prepareStatement(sql)
                ps.setInt(1, x)
                ps.execute()
                ps.close()
            })
            
            conn.close()
        })
        
        sc.stop()
        
    }
}
