package com.datageeks.broadcastDemo

import scala.util.Try
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

object TestBroadcastOfCountry {
  def main(args: Array[String]): Unit = {
    loadFileFromDisk("C://data/countries.txt") match {
      case Some(countries) => {
        print("Got Country")
        System.setProperty("hadoop.home.dir", "c://hadoop//")
        val sc = new SparkContext(new SparkConf().setAppName("Braodcast Demo").setMaster("local[4]"))
        val countriesCache = sc.broadcast(countries)
        val  countryNameRDD=sc.parallelize(countries.keys.toList)
        val counrtyRDD = sc.textFile("C://data/countries.txt")
        val resultRDD=searchCoutnryDetails(countryNameRDD, countriesCache, "A")
        countryNameRDD.foreach(element=>println(element))
        println(">>>> Searched results for companies starting with A := "+resultRDD.count())
      //resultRDD.foreach(entry=>println("Country Name:="+entry._1+"---|--Capital Value:= "+entry._2))
     sc.stop()
      }
      case None => println("Got Nothing")

    }

    def searchCoutnryDetails(counrtyRDD: RDD[String], cache: Broadcast[Map[String, String]], searchCharacter: String): RDD[(String, String)] = {
    counrtyRDD.filter(_.startsWith(searchCharacter)).map(country=>(country,cache.value(country)))
    }
  }
  def loadFileFromDisk(filename: String): Option[Map[String, String]] = {
    var countriesMap = Map[String, String]()
    Try {
      val bufferedReaderFromSource = Source.fromFile(filename)
      for (line <- bufferedReaderFromSource.getLines()) {
        println(line)
        val Array(country, capital) = line.split(",").map(_.trim())
        countriesMap += country -> capital
      }
      return Some(countriesMap)
    }.toOption

  }
}