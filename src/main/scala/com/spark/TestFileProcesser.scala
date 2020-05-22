package com.spark

import org.apache.spark.sql.SparkSession

object TestFileProcesser extends App {
  
  
    var spark = SparkSession.builder()
    .appName("Testing")
    .master("local")
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val fileRdd = spark.sparkContext.textFile("/home/dinesh/Documents/develop/spark/src/test/resources/test.txt", 1)
    
    val words = fileRdd.flatMap(line => line.split(" ")).map(w => (w,1)).reduceByKey(_+_)
    
    
    
  //  println(words.groupBy("Instagram").count())
    println(words.filter(f => !f._1.equals("Instagram")).count())
  
}