package com.atguigu

import java.text.DecimalFormat

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.Nil

/**
  * Author lzc
  * Date 2019-06-05 07:50
  */
object MyAggre {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        // 注册自定义函数
        spark.udf.register("my_avg", new MyAvg)
        
        val rdd = spark.sparkContext.parallelize(Seq(("lisi1", 21), ("lisi2", 30), ("lisi3", 25), ("lisi4", 15), ("lisi5", 18), ("lisi5", 18)))
        val df: DataFrame = rdd.toDF("name", "age")
        df.createOrReplaceTempView("user")
        spark.sql("select my_avg(age) from user").show
    }
}

class MyAvg extends UserDefinedAggregateFunction {
    /*
    指定将来聚集函数的输入的数据的类型       Double
     */
    override def inputSchema: StructType = {
        //        StructType(Array(StructField("inputColumn", DoubleType)))
        StructType(StructField("inputColumn", DoubleType) :: Nil)
    }
    
    /*
    缓冲类型   Double Int
    一个缓冲sum, 一个缓冲数量count
    
     */
    override def bufferSchema: StructType = {
        StructType(StructField("sum", DoubleType) :: StructField("count", IntegerType) :: Nil)
    }
    
    /*
    输出类型: double
     */
    override def dataType: DataType = StringType
    
    /*
    聚合函数的确定性: 相同的输入是否应该返回相同的输出  一般是true
     */
    override def deterministic: Boolean = true
    
    /*
     给缓冲初始化值
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 缓存 sum
        buffer(0) = 0d
        //缓存count
        buffer(1) = 0
    }
    
    /*
     age(10)
     每碰到一行就更新缓冲区
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 如果传进来的值不是空
        if (!input.isNullAt(0)) {
            // 计算sum
            buffer(0) = buffer.getDouble(0) + input.getDouble(0)
            buffer(1) = buffer.getInt(1) + 1
        }
    }
    
    /*
    分区间缓冲值的合并
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        if (!buffer2.isNullAt(0)) {
            buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
            buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
        }
    }
    
    /*
    聚合函数最终的返回值
     */
    override def evaluate(buffer: Row): Any = {
        new DecimalFormat(".00").format(buffer.getDouble(0) / buffer.getInt(1))
    }
}