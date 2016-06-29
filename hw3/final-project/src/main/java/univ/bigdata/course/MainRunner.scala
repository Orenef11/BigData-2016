package univ.bigdata.course

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object MainRunner {

  private var sc: SparkContext = null
  // this are the name of variables(productId, userId, profileName, helpfulness, score, time, summary, text)
  private var reviewMoviesRdd: RDD[(String, String, String, String, Double, String, String, String)] = null

  def main(args: Array[String]) {
    configSparkContext()
    var pathOutputFile: String = ""
    try
    {
      val file = Source.fromFile(args(0))
      val lines = file.getLines.toArray
      createRddFromFile(lines(0))
      for(i <- 2 to lines.size - 1)
        parsingFileToCommand(lines(i))
      pathOutputFile = lines(1)
      file.close()
    }
    catch
    {
        case ex: FileNotFoundException => println("Couldn't find that file.")
        case ex: IOException => println("Had an IOException trying to read that file")
    }

    sc.stop()

  }

  def configSparkContext(): Unit = {
    if (sc != null)
      sc.stop()

    val conf = new SparkConf().setMaster("local[*]").setAppName("master")
    sc = new SparkContext(conf)
  }

  def createRddFromFile(pathOfFile: String): Unit = {
    val fileRdd = sc.textFile(pathOfFile)
    val splitRdd = fileRdd.map(line => line.split("\t"))

    reviewMoviesRdd = splitRdd.flatMap(arr => {
      val productId = arr(0).split(": ")(1)
      val userId = arr(1).split(": ")(1)
      val profileName = arr(2).split(": ")(1)
      val helpfulness = arr(3).split(": ")(1)
      val score: Double = arr(4).split(": ")(1).toDouble
      val time = arr(5).split(": ")(1)
      val summary = arr(6).split(": ")(1)
      val text = arr(7).split("review/text: ")(1)
      productId.map(word => (productId, userId, profileName, helpfulness, score, time, summary, text)).distinct
    })

  }

  def parsingFileToCommand(commad: String): Unit ={
    val nameFunc: String = commad.split(" ")(0)
    nameFunc match {
      case "getTopKMoviesAverage" => getTopKMoviesAverage(commad.split(" ")(1).toInt)
      case "totalMoviesAverageScore" => totalMoviesAverageScore()
      case "movieWithHighestAverage" => movieWithHighestAverage()
      case "reviewCountPerMovieTopKMovies" => reviewCountPerMovieTopKMovies(commad.split(" ")(1).toInt)
      case "mostReviewedProduct" => mostReviewedProduct()
      case "moviesReviewWordsCount" => topYMoviesReviewTopXWordsCount(Int.MaxValue ,commad.split(" ")(1).toInt)
      case "topYMoviesReviewTopXWordsCount" => topYMoviesReviewTopXWordsCount(commad.split(" ")(1).toInt
        ,commad.split(" ")(2).toInt)
      case "mostPopularMovieReviewedByKUsers" => mostPopularMovieReviewedByKUsers(commad.split(" ")(1).toInt)
      case "topKHelpfullUsers" => topKHelpfullUsers(commad.split(" ")(1).toInt)
      case "moviesCount" => moviesCount()
      case _ => println("Error: function does not exist!\nPlease recheck the file integrity")
    }
  }

  def getTopKMoviesAverage(topK: Int): Unit ={
    val rddAverage = reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time,
    summary, text) => (productId, score)
    })

    var rddAverage1 = rddAverage.map { case (key, value) => (key, (value, 1L)) }
      .reduceByKey { case ((value1, count1), (value2, count2)) => (value1 + value2, count1 + count2) }
      .mapValues { case (value, count) => Math.round((value.toDouble / count.toDouble) * 100000) / 100000d }
    rddAverage1 = rddAverage1.sortBy(_._2, ascending = false)
      .sortByKey()

    rddAverage1.take(topK).foreach({ case (productId, score) => println(
      s"Movie{productId='$productId', score='$score'} ") })
  }

  def totalMoviesAverageScore(): Unit = {
    val rddTotalAverage = reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time,
    summary, text) => score})
    val countOfMovies: Long = rddTotalAverage.count()
    val totalAverage = rddTotalAverage.reduce(_ + _)

   println("Total average: " + Math.round((totalAverage / countOfMovies) * 100d) / 100d)
  }

  def movieWithHighestAverage(): Unit ={
    println("The movie with highest average: " + getTopKMoviesAverage(1))

    reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time,
    summary, text) => (productId, score)
    }).map { case (key, value) => (key, (value, 1L)) }
      .reduceByKey { case ((value1, count1), (value2, count2)) => (value1 + value2, count1 + count2) }
      .mapValues { case (value, count) => Math.round((value.toDouble / count.toDouble) * 100000) / 100000d }
      .sortBy(_._2, ascending = false)
      .take(1).foreach({ case (productId, score) => println("The movie with highest average: "+
      s"Movie{productId='$productId', score='$score'} ") })
  }

  def reviewCountPerMovieTopKMovies(topK: Int) ={
    reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time, summary, text) => productId})
      .map((_, 1L))
      .sortByKey()
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(topK).foreach({ case (productId, count) =>
      println(s"Movie product id = [$productId], reviews count [$count].") })


  }

  def mostReviewedProduct(): Unit ={
    reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time,
    summary, text) => productId})
      .map((_, 1L))
      .sortByKey()
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(1).foreach({ case (productId, count) =>
      println(s"The most reviewed movie product id is $productId") })
  }

//  def moviesReviewWordsCount(topK: Int): Unit ={
//    reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time, summary, text) => text})
//      .flatMap(text => text.split(" "))
//      .map(word => (word, 1L))
//      .reduceByKey(_ + _)
//      .sortByKey()
//      .sortBy(_._2, ascending = false)
//      .take(topK).foreach({case (word, count) => println(s"Word = [$word], number of occurrences [$count]") })
//  }

  def topYMoviesReviewTopXWordsCount(topKMoviesReview: Int, topKWords: Int): Unit ={
    var rdd = reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time, summary, text)
    => text})
    // take the topKMoviesReview reviews from the rdd
    val  tempList = rdd.take(Int.MaxValue).toList
   rdd = sc.parallelize(tempList).map(x => x)
      rdd.flatMap(text => text.split(" "))
      .map(word => (word, 1L))
      .reduceByKey(_ + _)
      .sortByKey()
      .sortBy(_._2, ascending = false)
      .take(topKWords).foreach({case (word, count) => println(s"Word = [$word], number of occurrences [$count]") })
  }


  def mostPopularMovieReviewedByKUsers(topK: Int): Unit ={
    val rdd = reviewMoviesRdd.map{ case (productId, userId, profileName, helpfulness, score, time, summary, text) =>
      (productId, (score, 1L))}
      .reduceByKey{ case (((value1, count1), (value2, count2))) =>
        (value1 + value2, count1 + count2) }
      .mapValues{case(average, count) => (count, average / count.toDouble)}
      .filter{case(productId, countAndAverageTuple) => countAndAverageTuple._1 >= topK }
      .map{ case (productId, countAndAverageTuple) => (productId, countAndAverageTuple._2)}
      .sortBy(_._2, ascending = false)
      .first()

    println("Most popular movie with highest average score, reviewed by at least " + topK.toString + " users " + rdd._1)


  }

  def topKHelpfullUsers(topK: Int): Unit = {
    val rdd = reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time, summary, text) =>
      (userId, helpfulness)})

    val rdd1 = rdd.flatMap(copuleString => {
      val temp = copuleString._2.split("/")
      val numerator = temp(0).toInt
      val denominator1 = temp(1).toInt
      temp.map(fraction => (copuleString._1, numerator, denominator1)).distinct
    })

      val rdd2 = rdd1.map { case (key, numerator, denominator) => (key, ((numerator, 1L), (denominator, 1L)))}
        .reduceByKey { case (((value1, count1), (value2, count2)), ((value3, count3), (value4, count4))) =>
          ((value1 + value3, count1 + count3), (value2 + value4, count2 + count4)) }
        .mapValues {case (numerator, denominator) => if(denominator._1.toDouble == 0) 0.0
        else Math.round((numerator._1.toDouble / denominator._1.toDouble)* 100000) / 100000d}

      rdd2.sortByKey()
        .sortBy(_._2, ascending = false)
        .take(topK)
        .foreach(fraction => println(s"User id = [" + fraction._1 + "], helpfulness [" + fraction._2 +"]."))

  }

  def moviesCount(): Unit ={
    val countOfMovies: Long = reviewMoviesRdd.map({ case (productId, userId, profileName, helpfulness, score, time, summary, text) => productId})
      .distinct().count()

    println(s"Total number of distinct movies reviewed [$countOfMovies].")
  }
}
