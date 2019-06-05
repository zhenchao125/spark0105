package com.atguigu

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ToDFByApi {
    def main(args: Array[String]): Unit = {
        // 通过api把rdd转成df
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        
        // RDD[Row]
        val rdd = spark.sparkContext.parallelize(Seq(("lisi1", 21), ("lisi2", 30), ("lisi3", 25), ("lisi4", 15), ("lisi5", 18), ("lisi5", 18)))
        val rowRDD: RDD[Row] = rdd.map {
            case (name, age) => Row(name, age)
        }
       
        
        val df: DataFrame = spark.createDataFrame(rowRDD, StructType(Array(StructField("name", StringType), StructField("age", IntegerType))))
        
        df.show
    }
}
