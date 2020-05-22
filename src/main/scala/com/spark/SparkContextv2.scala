package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

object SparkContextv2 {
  def main(args: Array[String]): Unit = {
    
    var ss1 = SparkSession
    .builder()
    .appName("Testing")
    .master("local")
    .getOrCreate()
    
      var ss2 = SparkSession
    .builder()
    .appName("Testing")
    .master("local")
    .getOrCreate()
    
    
    ss1.sparkContext.setLogLevel("ERROR")
    ss2.sparkContext.setLogLevel("ERROR")
    
    val array = Array(1, 2,3,4,5,6,7,8,9)
    
    val arrayRDD1 = ss1.sparkContext.parallelize(array, 3)
    val arrayRDD2 = ss2.sparkContext.parallelize(array, 3)
    
  //  arrayRDD1.collect().foreach(println)
  //  arrayRDD2.foreach(println)
    
    var dataDF = ss1.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/dinesh/Documents/develop/spark/src/test/resources/steaming-data/2010-12-01.csv")
    
    dataDF.printSchema()
    
   
    
    
  }
}