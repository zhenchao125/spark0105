package com.atguigu.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val parms = Map[String, String](
            ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop201:9092,hadoop202:9092,hadoop203:9092"
            /*ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"*/
        )
        val dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, parms, Set("spark0105"))
        val wordCount: DStream[(String, Int)] = dstream.flatMap(_._2.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
        
        dstream.print
        
        ssc.start()
        ssc.awaitTermination()
    }
}
