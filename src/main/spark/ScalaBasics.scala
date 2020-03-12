package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ScalaBasics {
  
  def main(args: Array[String]): Unit = {
    
    var sparkconfig =  new SparkConf().setAppName("Histogram").setMaster("local")
    
    var sc = new Sparkcontext(sparkconfig)
    
    var data = Array(1,2,3,4,5,6,7,8,9,0)
    
    var rdd = sc.parallelize(data)
    
    rdd.foreach(println)
    
    
    
  }
  
}