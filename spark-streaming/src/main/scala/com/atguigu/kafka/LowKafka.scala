package com.atguigu.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-06-06 11:03
  */
object LowKafka {
    /**
      * 根据指定的参数读取offsets
      *
      * @param kafkaCluster
      * @param topic
      * @param group
      * @return
      */
    def readOffsets(kafkaCluster: KafkaCluster, topic: String, group: String): Map[TopicAndPartition, Long] = {
        // 最终的返回结果
        var result: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
        // 获取到所有的主题和分区的offset信息
        val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
        // 判断是否有正确的数据
        if (topicAndPartitionEither.isRight) {
            // 获取到topic和分区信息
            val topicAndPartitionSet: Set[TopicAndPartition] = topicAndPartitionEither.right.get
            // 获取offset
            val topicAndPartition2LongEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPartitionSet)
            if (topicAndPartition2LongEither.isRight) { // 如果有曾经的消费记录
                val topicAndPartition2LongMap: Map[TopicAndPartition, Long] = topicAndPartition2LongEither.right.get
                result ++= topicAndPartition2LongMap
            } else { // 表示第一次消费, 把每个分区的offset都置于0
                topicAndPartitionSet.foreach(topicAndPartition => {
                    result += topicAndPartition -> 0
                })
            }
        }
        result
    }
    
    /*
    保存offsets
     */
    def saveOffsets(kafkaCluster: KafkaCluster, group: String, topic: String, dstream: InputDStream[String]) = {
        //
        dstream.foreachRDD(rdd => {
            var result: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
            // 把rdd强转成HasOffsetRanges, 然后取出OffsetRanges
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            val ranges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
            ranges.foreach(offsetRnage => {
                result += offsetRnage.topicAndPartition() -> offsetRnage.untilOffset
            })
            
            kafkaCluster.setConsumerOffsets(group, result)
        })
    }
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val parms = Map[String, String](
            ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop201:9092,hadoop202:9092,hadoop203:9092"
        )
        val kafkaCluster = new KafkaCluster(parms)
        // 读offsets
        var offsets = readOffsets(kafkaCluster, "spark0105", "bigdata")
        val dstream =
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
                ssc,
                parms,
                offsets,
                (handler: MessageAndMetadata[String, String]) => handler.message() // 直接把kafka的信息(value)封装到DStream中
            )
        
        val wordCount: DStream[(String, Int)] = dstream.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
        
        wordCount.print
        // 写offsets
        saveOffsets(kafkaCluster, "bigdata", "spark0105", dstream)
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
使用低级消费者消费数据:
    需要自己去维护offset
    
 
 */