package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.streaming.OutputMode

object SparkStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val retailDataSchema = new StructType()
      .add("InvoiceNo", IntegerType)
      .add("StockCode", IntegerType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamingData = spark
      .readStream
      .schema(retailDataSchema)
      .csv("/home/dinesh/Documents/develop/spark/src/test/resources/steaming-data")

    val filteredData = streamingData.filter("Country = 'France'")

    val query = filteredData.writeStream
      .format("console")
      .queryName("filteredByCountry")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()

  }
}