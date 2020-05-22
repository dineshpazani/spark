package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

case class Data1(PassengerId: Integer, Survived: Integer, Pclass: Integer,
                 Name: String, Sex: String, Age: Integer,
                 SibSp: Integer, Parch: Integer, Ticket: String,
                 Fare: Option[Double], Cabin: Option[String], Embarked: Option[String])

case class MoviesCredit(movie_id: String, title: String, cast: String, crew: String)

case class Movies(budget: String, genres: String,homepage: String,id: String,keywords: String,original_language: String,
    original_title: String,overview: String,popularity: String,production_companies: String,production_countries: String,
    release_date: String,revenue: String,runtime: String,spoken_languages: String,status: String,
    tagline: String,title: String,vote_average: String,vote_count: String
)

object WordCountProgram {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Testing")
      .master("local")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val csv = spark.sparkContext.textFile("/home/dinesh/Documents/develop/spark/src/test/resources/tmdb-movie-metadata/tmdb_5000_movies.csv", 1)

    csv.take(2).foreach(println)

    val dataRDD1 = spark.read.schema(Encoders.product[Data1].schema).option("header", true)
      .option("delimiter", ",")
      .csv("/home/dinesh/Documents/develop/spark/src/test/resources/train.csv").as[Data1]

    val moviesCreditRdd = spark.read.schema(Encoders.product[MoviesCredit].schema).option("header", true)
      .option("delimiter", ",")
      .csv("/home/dinesh/Documents/develop/spark/src/test/resources/tmdb-movie-metadata/tmdb_5000_credits.csv").as[MoviesCredit]
    
    val moviesRdd = spark.read.schema(Encoders.product[Movies].schema).option("header", true)
    .option("delimiter", ",")
    .csv("/home/dinesh/Documents/develop/spark/src/test/resources/tmdb-movie-metadata/tmdb_5000_movies.csv").as[Movies]
    
   // moviesCreditRdd.filter(condition)
    println(moviesRdd.count())
   
    println(moviesRdd.filter(obj => obj.budget == "225000000").count())
    val whereMovies = moviesRdd.where(moviesRdd("budget") === "225000000")
    whereMovies.show()
    
     val g = moviesRdd.groupBy("budget")
     g.count().show()
     
     
     
     spark.stop()

  }
}