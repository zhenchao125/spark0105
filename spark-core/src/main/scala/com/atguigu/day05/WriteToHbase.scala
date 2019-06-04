package com.atguigu.day05

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WriteToHbase {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop201,hadoop202,hadoop203")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
        // 通过job来设置输出的格式的类
        val job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        
        val initialRDD =
            sc.parallelize(List(("100", "apple", "11"), ("200", "banana", "12"), ("300", "pear", "13")))
        // (rowKey, Put)
        val resultRDD: RDD[(ImmutableBytesWritable, Put)] = initialRDD.map {
            case (rowKey, name, weight) => {
                val put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weight"), Bytes.toBytes(weight))
                (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
            }
        }
        resultRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
}
