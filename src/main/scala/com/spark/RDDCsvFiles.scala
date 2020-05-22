package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RDDCsvFiles {

  def main(args: Array[String]): Unit = {

    var sparkconfig = new SparkConf().setAppName("Histogram").setMaster("local")

    var csvRDD = new SparkContext(sparkconfig).textFile("src/test/resources/train.csv");
    
    //get count
    println(csvRDD.count())
    
    
    // take first 10 lines
    csvRDD.take(10).foreach(println)
    
    //with out header
    var header = csvRDD.first()    
    var csvRddWithoutHeader = csvRDD.filter(_ != header)    
    csvRddWithoutHeader.foreach(println)
    
   
   // csv take particular column to rdd
   var csvArray = csvRddWithoutHeader.map(line =>{
     
     var csvArray = line.split(" ")
     
     if(csvArray.length>4)
       Array(csvArray(0), csvArray(1), csvArray(3)).mkString(" ")
      else
        println(csvArray(0))
     
   }).take(10).foreach(println)
  //csvArray.foreach(println)
   
   
   //Save file in csv:
    
    
    

  }

}