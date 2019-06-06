package com.atguigu

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-06-05 18:31
  */
object WordCountWidown3 {
    def main(args: Array[String]): Unit = {
        //1. 创建 StreamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(2))
        ssc.checkpoint("./ck4")
        //2. 获取DStream
        val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 20000)
        //3. 对DStream进行处理
        val wordOne: DStream[(String, Int)] = lineStream.flatMap(_.split("\\W+")).map((_, 1))
        val result = wordOne.reduceByKeyAndWindow(_ + _, _ - _, Seconds(6), Seconds(6), filterFunc = kv => kv._2 != 0)
        // 4. 显示处理的结果
        //        result.saveAsTextFiles("word", "txt")
        result.foreachRDD(rdd => {
            Class.forName("com.mysql.jdbc.Driver")
            val conn: Connection = DriverManager.getConnection("jdbc:mysql://hadoop201:3306/rdd", "root", "aaa")
            // 连接
            val sql = "insert into user values(?)"
            rdd.collect.foreach {
                case (word, count) =>
                    val ps: PreparedStatement = conn.prepareStatement(sql)
                    ps.setInt(1, count)
                    ps.execute()
                    ps.close()
            }
            conn.close()
        })
        
        
        // 5. 启动ssc
        ssc.start()
        // 6. 让应用一直不停
        ssc.awaitTermination()
    }
}
